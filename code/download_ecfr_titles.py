import requests
import os
import json
from markitdown import MarkItDown
from wakepy import keep
from bs4 import XMLParsedAsHTMLWarning, Tag
import warnings
import duckdb
import logging
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv
from tqdm import tqdm

from utils import get_standard_timestamp
from upsert_to_db import upsert_to_db, batch_upsert_to_db

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
md = MarkItDown(enable_plugins=False)

# Load environment variables
load_dotenv()

TITLE_NUMBERS = [27, 21, 19, 26, 31]
DOWNLOAD_DIR = "data"

# Local DuckDB configuration
LOCAL_DB_PATH = os.path.join(DOWNLOAD_DIR, "ecfr_data.duckdb")

# Batch processing configuration
BATCH_SIZE = 100  # Number of records to process in each batch

# Configure logging with rotating file handler and console handler
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# Ensure logs directory exists
os.makedirs('logs', exist_ok=True)
# Create rotating file handler - INFO level
file_handler = RotatingFileHandler(
    'logs/download_ecfr_titles.log', 
    maxBytes=10*1024*1024,  # 10MB
    backupCount=2,          # Keep 2 backup files (total ~30MB max)
    encoding='utf-8'
)
file_handler.setLevel(logging.INFO)
# Create console handler - ERROR level  
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.ERROR)
# Create formatter
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)
# Add handlers to logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)

def get_local_connection():
    """Get (and initialize if needed) a connection to the local DuckDB database."""
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    initializing = not os.path.exists(LOCAL_DB_PATH)
    try:
        conn = duckdb.connect(LOCAL_DB_PATH)
        if initializing:
            logger.info(f"Created new local DuckDB database at {LOCAL_DB_PATH}")
        # Ensure required tables exist using the SQL schema file if present
        schema_sql_path = os.path.join("docs", "ecfr_data_model.sql")
        if os.path.exists(schema_sql_path):
            with open(schema_sql_path, 'r', encoding='utf-8') as f:
                schema_sql = f.read()
            # Split on semicolons while preserving statements (simple split sufficient here)
            for stmt in [s.strip() for s in schema_sql.split(';') if s.strip()]:
                try:
                    conn.execute(stmt)
                except Exception as stmt_e:
                    logger.debug(f"Skipping statement due to error (may be COMMENT or already exists): {stmt_e}")
        logger.info(f"Connected to local DuckDB at {LOCAL_DB_PATH}")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to local DuckDB at {LOCAL_DB_PATH}: {e}")
        raise


# --- API & METADATA LOGIC ---
def get_titles_metadata():
    url = "https://www.ecfr.gov/api/versioner/v1/titles"
    response = requests.get(url)
    response.raise_for_status()
    titles = response.json()["titles"]
    filtered_titles = [title for title in titles if title["number"] in TITLE_NUMBERS]
    return filtered_titles

def get_titles_metadata_and_write_to_db(conn):
    try:
        url = "https://www.ecfr.gov/api/versioner/v1/titles"
        logger.info(f"Fetching titles metadata from {url}")
        response = requests.get(url)
        response.raise_for_status()
        titles = response.json()["titles"]
        filtered_titles = [title for title in titles if title["number"] in TITLE_NUMBERS]
        logger.info(f"Filtered to {len(filtered_titles)} relevant titles: {[t['number'] for t in filtered_titles]}")
        
        # Prepare records for batch processing
        title_records = []
        for title in tqdm(filtered_titles, desc="Processing titles metadata", unit="title"):
            # First check if title already exists to preserve title_details_download_date
            existing_result = conn.execute(
                "SELECT title_details_download_date FROM titles WHERE title_number = ?",
                [title["number"]]
            ).fetchone()
            
            existing_download_date = existing_result[0] if existing_result else None
            
            record = {
                "title_number": title["number"],
                "title_label": title.get("name"),
                "latest_issue_date": title.get("latest_issue_date"),
                "up_to_date_as_of": title.get("up_to_date_as_of"),
                "reserved": title.get("reserved"),
                "title_details_download_date": existing_download_date,  # Preserve existing date
            }
            title_records.append(record)
            logger.debug(f"Prepared record for title {title['number']}: {title.get('name')}")
        
        # Batch upsert all title records
        if title_records:
            try:
                records_processed = batch_upsert_to_db(conn, title_records, 'titles', 
                                                     conflict_key='title_number', batch_size=BATCH_SIZE)
                logger.info(f"Successfully batch upserted {records_processed} title records")
            except Exception as e:
                logger.warning(f"Batch upsert failed, falling back to individual inserts: {e}")
                # Fallback to individual processing
                for record in title_records:
                    try:
                        upsert_to_db(conn, record, "titles", "title_number")
                        logger.debug(f"Individual upsert successful for title {record['title_number']}")
                    except Exception as individual_error:
                        logger.error(f"Individual upsert failed for title {record['title_number']}: {individual_error}")
        
        return filtered_titles
    except Exception as e:
        logger.error(f"Error fetching titles metadata: {e}")
        raise

def should_download_title_details(conn, title_obj):
    """
    Check if title details should be downloaded based on latest_issue_date vs current download date.
    
    Args:
        conn: Database connection object
        title_obj (dict): Title metadata object with latest_issue_date
        
    Returns:
        bool: True if details should be downloaded, False if current data is up to date
    """
    return True # Temporary until details are implemented in parsing
    try:
        title_number = title_obj['number']
        latest_issue_date = title_obj.get('latest_issue_date')
        
        if not latest_issue_date:
            logger.info(f"Title {title_number}: No latest_issue_date available, downloading details")
            return True
        
        # Query current title_details_download_date from database
        result = conn.execute(
            "SELECT title_details_download_date FROM titles WHERE title_number = ?",
            [title_number]
        ).fetchone()
        
        if not result or not result[0]:
            logger.info(f"Title {title_number}: No previous download date, downloading details")
            return True
        
        current_download_date = result[0]
        
        # Compare dates (latest_issue_date should be newer than download date to trigger download)
        # Convert datetime object to date string if needed
        if isinstance(current_download_date, str):
            current_date_str = current_download_date[:10]  # Extract YYYY-MM-DD part
        else:
            # Handle datetime object
            current_date_str = current_download_date.strftime('%Y-%m-%d')
        
        if latest_issue_date > current_date_str:
            logger.info(f"Title {title_number}: Latest issue date {latest_issue_date} is newer than download date {current_date_str}, downloading details")
            return True
        else:
            logger.info(f"Title {title_number}: Current data is up to date (latest_issue_date: {latest_issue_date}, last_download: {current_date_str}), skipping")
            return False
            
    except Exception as e:
        logger.error(f"Error checking download status for Title {title_obj.get('number', 'unknown')}: {e}")
        # Default to downloading on error
        return True

def get_parts_and_structure(title_obj, download_dir, conn):
    """
    Download and process eCFR title structure and optionally full text.
    
    Args:
        title_obj (dict): Title metadata object
        download_dir (str): Directory to save downloaded files
        conn: Database connection object
    """
    try:
        title_number = title_obj['number']
        up_to_date_as_of = title_obj['up_to_date_as_of']
        
        # Structure JSON
        structure_url = f"https://www.ecfr.gov/api/versioner/v1/structure/{up_to_date_as_of}/title-{title_number}.json"
        logger.info(f"Fetching structure from {structure_url}")
        structure_response = requests.get(structure_url)
        structure_response.raise_for_status()
        structure_json = structure_response.json()
        
        # Save pretty JSON for reference
        structure_file = f"{download_dir}/ecfr_title-{title_number}-structure.json"
        with open(structure_file, "w", encoding="utf-8") as f:
            json.dump(structure_json, f, indent=2, ensure_ascii=False)
        logger.info(f"Downloaded structure for Title {title_number}")
        
        # Flatten the structure with improved CFR reference calculation
        flattened = flatten_all_elements_with_full_hierarchy(structure_json)
        flattened_file = f"{download_dir}/ecfr_title-{title_number}-structure-flat.json"
        with open(flattened_file, "w", encoding="utf-8") as f:
            json.dump(flattened, f, indent=2, ensure_ascii=False)
        logger.info(f"Extracted {len(flattened)} elements with full hierarchy context and hierarchy level.")

        # Download full XML text (currently commented out as it's not processed)
        full_url = f"https://www.ecfr.gov/api/versioner/v1/full/{up_to_date_as_of}/title-{title_number}.xml"
        logger.info(f"Downloading full XML from: {full_url}")
        
        # Download and save XML file
        xml_file_path = f"{download_dir}/ecfr_title-{title_number}-full.xml"
        try:
            xml_response = requests.get(full_url)
            xml_response.raise_for_status()
            with open(xml_file_path, 'w', encoding='utf-8') as f:
                f.write(xml_response.text)
            logger.info(f"Downloaded XML file for Title {title_number}")
            
            # Parse the XML for numbered DIV elements
            div_elements = parse_xml_divs_with_numbers(xml_file_path)
            logger.info(f"Parsed {len(div_elements)} numbered DIV elements from XML")
            
            # Save parsed DIV data as JSON for reference
            div_file_path = f"{download_dir}/ecfr_title-{title_number}-div-elements.json"
            with open(div_file_path, 'w', encoding='utf-8') as f:
                json.dump(div_elements, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved parsed DIV elements to {div_file_path}")
            
        except requests.RequestException as e:
            logger.error(f"Error downloading XML for Title {title_number}: {e}")
        except Exception as e:
            logger.error(f"Error processing XML for Title {title_number}: {e}")
        
        # Insert into title_details table
        # Filter out the 'title' record from flattened (it's already in the titles table)
        filtered_flattened = [item for item in flattened if item.get("hierarchy_type") != "title"]
        
        if not filtered_flattened:
            logger.warning(f"No detail records found for Title {title_number} after filtering")
            return
        
        # Prepare records for batch processing
        detail_records = []
        skipped_records = 0
        
        # Process database insertions with progress bar
        for item in tqdm(filtered_flattened, desc=f"Preparing Title {title_number} records", unit="record", leave=False):
            try:
                # Map fields to DB schema with validation
                record = {
                    "cfr_ref": item.get("cfr_ref"),
                    "reg_text": None,  # Full text not processed in this version
                    "reg_text_download_date": None,
                    "hierarchy_type": item.get("hierarchy_type"),
                    "hierarchy_level": item.get("hierarchy_level"),
                    "is_leaf_node": item.get("is_leaf_node", False),
                    "reserved": item.get("reserved", False),
                    "order_id": item.get("order_id"),
                    "title_number": title_number,
                    "chapter_id": item.get("chapter_identifier"),
                    "chapter_label": item.get("chapter_label"),
                    "subchapter_id": item.get("subchapter_identifier"),
                    "subchapter_label": item.get("subchapter_label"),
                    "part_id": item.get("part_identifier"),
                    "part_label": item.get("part_label"),
                    "subpart_id": item.get("subpart_identifier"),
                    "subpart_label": item.get("subpart_label"),
                    "section_id": item.get("section_identifier"),
                    "section_label": item.get("section_label"),
                    "appendix_id": item.get("appendix_identifier"),
                    "appendix_label": item.get("appendix_label"),
                    "subject_grp_id": item.get("subject_group_identifier"),
                    "subject_grp_label": item.get("subject_group_label"),
                }
                
                # Validate required fields
                if not record["cfr_ref"]:
                    logger.warning(f"Skipping record with missing cfr_ref: {item.get('hierarchy_type')} at level {item.get('hierarchy_level')}")
                    skipped_records += 1
                    continue
                
                detail_records.append(record)
                
            except Exception as e:
                logger.error(f"Error preparing record for {item.get('cfr_ref', 'unknown')}: {e}")
                continue
        
        # Batch upsert detail records
        successful_inserts = 0
        if detail_records:
            try:
                records_processed = batch_upsert_to_db(conn, detail_records, 'title_details', 
                                                     conflict_key='cfr_ref', batch_size=BATCH_SIZE)
                successful_inserts = records_processed
                logger.info(f"Successfully batch upserted {records_processed} title detail records for Title {title_number}")
            except Exception as e:
                logger.warning(f"Batch upsert failed for Title {title_number}, falling back to individual inserts: {e}")
                # Fallback to individual processing
                for record in detail_records:
                    try:
                        upsert_to_db(conn, record, "title_details", "cfr_ref")
                        successful_inserts += 1
                    except Exception as individual_error:
                        logger.error(f"Individual upsert failed for {record.get('cfr_ref', 'unknown')}: {individual_error}")
        
        logger.info(f"Successfully processed {successful_inserts}/{len(filtered_flattened)} title detail records for Title {title_number} (skipped {skipped_records})")
        
        # Update title_details_download_date in titles table
        conn.execute(
            "UPDATE titles SET title_details_download_date = ? WHERE title_number = ?",
            [get_standard_timestamp(), title_number]
        )
        conn.commit()
        logger.info(f"Updated title_details_download_date for Title {title_number}")
        
    except requests.RequestException as e:
        logger.error(f"Network error processing Title {title_obj.get('number', 'unknown')}: {e}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"JSON parsing error for Title {title_obj.get('number', 'unknown')}: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error processing Title {title_obj.get('number', 'unknown')}: {e}")
        raise

def parse_xml_divs_with_numbers(xml_file_path):
    """
    Parse XML file and extract DIV elements with numbers in their tag names.
    
    Args:
        xml_file_path (str): Path to the XML file to parse
        
    Returns:
        list: List of dictionaries containing div information with:
            - div_tag: The complete div tag name (e.g., 'DIV1', 'DIV2', etc.)
            - div_number: The number extracted from the div tag
            - text_content: Text content between the div tags (stripped of whitespace)
            - All XML attributes are flattened to the top level
    """
    try:
        import re
        from bs4 import BeautifulSoup
        
        logger.info(f"Parsing XML file: {xml_file_path}")
        
        with open(xml_file_path, 'r', encoding='utf-8') as file:
            xml_content = file.read()
        
        # Parse XML with BeautifulSoup
        soup = BeautifulSoup(xml_content, 'xml')
        
        # Find all DIV elements that have numbers in their tag names
        div_pattern = re.compile(r'^DIV\d+$', re.IGNORECASE)
        numbered_divs = soup.find_all(div_pattern)
        
        result = []
        
        for div in tqdm(numbered_divs, desc="Processing numbered DIV elements", unit="div", leave=False):
            # Extract the number from the DIV tag
            div_tag = getattr(div, "name", None)
            if div_tag:
                div_tag = div_tag.upper()
            else:
                div_tag = ""
            div_number_match = re.search(r'DIV(\d+)', div_tag, re.IGNORECASE)
            div_number = int(div_number_match.group(1)) if div_number_match else None
            
            # Get direct text content (excluding nested tags)
            text_content = ""
            head_content = ""
            secauth_content = ""
            cita_content = ""
            
            # Extract HEAD element content separately
            if isinstance(div, Tag):
                head_elements = div.find_all('HEAD')
                if head_elements:
                    head_content = head_elements[0].get_text(strip=True)
                
                # Extract SECAUTH element content separately
                secauth_elements = div.find_all('SECAUTH')
                if secauth_elements:
                    secauth_content = secauth_elements[0].get_text(strip=True)
                
                # Extract CITA element content separately
                cita_elements = div.find_all('CITA')
                if cita_elements:
                    cita_content = cita_elements[0].get_text(strip=True)
            
            text_content_candidate = div.get_text(strip=True)
            if text_content_candidate:
                text_content = text_content_candidate
            else:
                # Get all content excluding nested DIV elements with numbers and special elements
                excluded_elements = {'HEAD', 'SECAUTH', 'CITA'}
                
                # Create a copy of the div for processing
                import copy
                temp_div = copy.deepcopy(div)
                
                # Remove excluded elements and nested numbered DIVs
                elements_to_remove = []
                if isinstance(temp_div, Tag):
                    for element in temp_div.find_all():
                        element_name = getattr(element, "name", None)
                        if (element_name and 
                            (div_pattern.match(element_name) or 
                             element_name.upper() in excluded_elements)):
                            elements_to_remove.append(element)
                
                for element in elements_to_remove:
                    element.decompose()
                
                # Convert the cleaned XML/HTML to markdown
                try:
                    # Get the inner HTML content (without the outer DIV tag)
                    # Safely get inner content depending on the type of temp_div
                    if isinstance(temp_div, Tag) and hasattr(temp_div, 'contents') and isinstance(temp_div.contents, list):
                        inner_content = ''.join(str(child) for child in temp_div.contents)
                    elif isinstance(temp_div, Tag) and hasattr(temp_div, 'string') and temp_div.string is not None:
                        inner_content = temp_div.string
                    else:
                        inner_content = ''
                    
                    if inner_content.strip():
                        # Create a temporary file for MarkItDown to process
                        import tempfile
                        with tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False, encoding='utf-8') as temp_file:
                            # Write proper HTML structure
                            temp_file.write(f"<!DOCTYPE html><html><head><title>CFR Content</title></head><body>{inner_content}</body></html>")
                            temp_file_path = temp_file.name
                        
                        try:
                            logger.debug(f"Converting temp file to markdown for {div_tag}: {temp_file_path}")
                            markdown_result = md.convert(temp_file_path)
                            if markdown_result and markdown_result.text_content:
                                text_content = markdown_result.text_content.strip()
                                logger.debug(f"Markdown result for {div_tag}: {text_content[:200]}...")
                            else:
                                # Fallback to plain text extraction
                                text_content = temp_div.get_text(separator=' ', strip=True)
                        finally:
                            # Clean up temp file
                            try:
                                os.unlink(temp_file_path)
                            except Exception:
                                pass
                    else:
                        text_content = ""
                        
                except Exception as e:
                    logger.warning(f"Could not convert to markdown for div {div_tag}: {e}")
                    # Fallback to plain text extraction
                    text_content = temp_div.get_text(separator=' ', strip=True)
                
                # Clean up any remaining extra whitespace
                if text_content:
                    text_content = ' '.join(text_content.split()).strip()
            
            # Start with basic div info
            div_info = {
                'div_tag': div_tag,
                'div_number': div_number,
                'text_content': text_content,
                'head_content': head_content,
                'secauth_content': secauth_content,
                'cita_content': cita_content
            }
            
            # Flatten all XML attributes to the top level
            if isinstance(div, Tag) and div.attrs:
                div_info.update(dict(div.attrs))
            
            result.append(div_info)
            
        logger.info(f"Extracted {len(result)} numbered DIV elements from XML")
        return result
        
    except FileNotFoundError:
        logger.error(f"XML file not found: {xml_file_path}")
        return []
    except Exception as e:
        logger.error(f"Error parsing XML file {xml_file_path}: {e}")
        return []

def calculate_cfr_ref(item):
    """
    Calculate CFR reference identifier for a hierarchy item.
    
    Args:
        item (dict): Hierarchy item with type and identifier fields
        
    Returns:
        str: Formatted CFR reference identifier
    """
    hierarchy_type = item.get("hierarchy_type")
    title_id = item.get("title_identifier", "")
    
    if not title_id:
        logger.warning(f"Missing title_identifier for {hierarchy_type} item")
        return f"CFR {hierarchy_type}"
    
    base_ref = f"{title_id} CFR"
    
    if hierarchy_type == "title":
        return base_ref
    elif hierarchy_type == "chapter":
        chapter_id = item.get("chapter_identifier", "")
        return f"{base_ref} ch{chapter_id}" if chapter_id else base_ref
    elif hierarchy_type == "subchapter":
        chapter_id = item.get("chapter_identifier", "")
        subchapter_id = item.get("subchapter_identifier", "")
        if chapter_id and subchapter_id:
            return f"{base_ref} ch{chapter_id}-{subchapter_id}"
        return base_ref
    elif hierarchy_type == "part":
        part_id = item.get("part_identifier", "")
        return f"{base_ref} pt{part_id}" if part_id else base_ref
    elif hierarchy_type == "subpart":
        part_id = item.get("part_identifier", "")
        subpart_id = item.get("subpart_identifier", "")
        if part_id and subpart_id:
            return f"{base_ref} pt{part_id}-{subpart_id}"
        return base_ref
    elif hierarchy_type == "section":
        section_id = item.get("section_identifier", "")
        return f"{base_ref} §{section_id}" if section_id else base_ref
    elif hierarchy_type == "appendix":
        # Handle appendix attached to section or part
        if "section_identifier" in item and item["section_identifier"]:
            section_id = item["section_identifier"]
            appendix_id = item.get("appendix_identifier", "")
            return f"{base_ref} §{section_id} ( {appendix_id})" if appendix_id else f"{base_ref} §{section_id}"
        elif "part_identifier" in item and item["part_identifier"]:
            part_id = item["part_identifier"]
            appendix_id = item.get("appendix_identifier", "")
            return f"{base_ref} pt{part_id} ( {appendix_id})" if appendix_id else f"{base_ref} pt{part_id}"
        else:
            appendix_id = item.get("appendix_identifier", "")
            return f"{base_ref} ( {appendix_id})" if appendix_id else base_ref
    elif hierarchy_type == "subject_group":
        subj_grp_id = item.get("subject_group_identifier", "")
        subj_grp_suffix = f" (Subj Grp {subj_grp_id})" if subj_grp_id else ""
        
        # Build base reference for subject group context
        if "appendix_identifier" in item and item["appendix_identifier"]:
            section_id = item.get("section_identifier", "")
            appendix_id = item["appendix_identifier"]
            base = f"{base_ref} §{section_id} ( {appendix_id})" if section_id else f"{base_ref} ( {appendix_id})"
        elif "section_identifier" in item and item["section_identifier"]:
            section_id = item["section_identifier"]
            base = f"{base_ref} §{section_id}"
        elif "subpart_identifier" in item and item["subpart_identifier"]:
            part_id = item.get("part_identifier", "")
            subpart_id = item["subpart_identifier"]
            base = f"{base_ref} pt{part_id}-{subpart_id}" if part_id else f"{base_ref} (Subpart {subpart_id})"
        elif "part_identifier" in item and item["part_identifier"]:
            part_id = item["part_identifier"]
            base = f"{base_ref} pt{part_id}"
        elif "subchapter_identifier" in item and item["subchapter_identifier"]:
            chapter_id = item.get("chapter_identifier", "")
            subchapter_id = item["subchapter_identifier"]
            base = f"{base_ref} ch{chapter_id}-{subchapter_id}" if chapter_id else f"{base_ref} (Subchapter {subchapter_id})"
        else:
            base = base_ref
            
        return f"{base}{subj_grp_suffix}"
    else:
        logger.warning(f"Unknown hierarchy_type: {hierarchy_type}")
        return f"{base_ref} ({hierarchy_type})"


# --- FLATTENING LOGIC (improved) ---
def flatten_all_elements_with_full_hierarchy(node, results=None, parent_chain=None, level=0, order_id=[1]):
    if results is None:
        results = []
    if parent_chain is None:
        parent_chain = []

    # Remove unwanted keys from the current node
    remove_list = ["size", "volumes", "descendant_range"]
    for key in remove_list:
        if key in node:
            del node[key]

    # Build a combined dict of all parent fields up the hierarchy
    combined_parent_fields = {}
    for ancestor in parent_chain:
        for k, v in ancestor.items():
            if k != "children":
                prefix = ancestor.get("type", "root")
                combined_parent_fields[f"{prefix}_{k}"] = v

    # Add current node's fields (without children)
    element = {}
    for k, v in node.items():
        if k not in ["children", "type"]:
            prefix = node.get("type", "root")
            element[f"{prefix}_{k}"] = v
    
    # Merge in all parent fields up the hierarchy
    element.update(combined_parent_fields)
    
    # Add hierarchy metadata
    element["hierarchy_level"] = level
    element["hierarchy_type"] = node.get("type")
    element["order_id"] = order_id[0]
    order_id[0] += 1
    
    # Calculate if this is a leaf node (has no children)
    element["is_leaf_node"] = len(node.get("children", [])) == 0
    
    # Calculate CFR reference for this element
    element["cfr_ref"] = calculate_cfr_ref(element)
    
    results.append(element)

    # Recurse into children, passing down the full parent chain and incrementing level
    for child in node.get("children", []):
        flatten_all_elements_with_full_hierarchy(child, results, parent_chain + [node], level=level+1, order_id=order_id)

    return results

if __name__ == "__main__":
    with keep.running():
        os.makedirs(DOWNLOAD_DIR, exist_ok=True)
        logger.info("Starting eCFR titles download process (local DuckDB mode)...")
        conn = get_local_connection()
        try:
            titles_with_dates = get_titles_metadata_and_write_to_db(conn)
            logger.info(f"Retrieved metadata for {len(titles_with_dates)} titles")
            for title in tqdm(titles_with_dates, desc="Processing eCFR titles", unit="title"):
                download_dir = f"{DOWNLOAD_DIR}/ecfr_title-{title['number']}"
                os.makedirs(download_dir, exist_ok=True)
                if should_download_title_details(conn, title):
                    logger.info(
                        f"Title {title['number']}: {title['name']} (Up to date as of {title['up_to_date_as_of']}) Fetching parts and structure..."
                    )
                    get_parts_and_structure(title, download_dir, conn)
                    logger.info(f"Successfully processed Title {title['number']}")
                else:
                    logger.info(f"Title {title['number']}: Skipping download - current data is up to date")
            logger.info("All titles processed and written to local DuckDB database.")
        finally:
            conn.close()
            logger.info("Connection closed.")

