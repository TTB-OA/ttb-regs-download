CREATE TABLE IF NOT EXISTS "titles" (
  "title_number" int PRIMARY KEY,
  "title_label" varchar,
  "latest_issue_date" date,
  "up_to_date_as_of" date,
  "reserved" boolean,
  "title_details_download_date" datetime
);

CREATE TABLE IF NOT EXISTS "title_details" (
  "cfr_ref" varchar PRIMARY KEY,
  "reg_text" text,
  "reg_text_download_date" datetime,
  "hierarchy_type" varchar,
  "hierarchy_level" int,
  "is_leaf_node" boolean,
  "reserved" boolean,
  "order_id" int,
  "title_number" int,
  "chapter_id" varchar,
  "chapter_label" varchar,
  "subchapter_id" varchar,
  "subchapter_label" varchar,
  "part_id" varchar,
  "part_label" varchar,
  "subpart_id" varchar,
  "subpart_label" varchar,
  "section_id" varchar,
  "section_label" varchar,
  "appendix_id" varchar,
  "appendix_label" varchar,
  "subject_grp_id" varchar,
  "subject_grp_label" varchar,
);

COMMENT ON COLUMN "titles"."title_number" IS 'the title number of the eCFR, e.g., 27 for Title 27';
COMMENT ON COLUMN "titles"."title_details_download_date" IS 'the date the title details / structure was last downloaded from the eCFR';
COMMENT ON COLUMN "title_details"."cfr_ref" IS 'the CFR reference, generated from the components of the item';
COMMENT ON COLUMN "title_details"."reg_text" IS 'the text of the regulation, as it appears in the eCFR';
COMMENT ON COLUMN "title_details"."reg_text_download_date" IS 'the date the regulation text was last downloaded from the eCFR';
COMMENT ON COLUMN "title_details"."hierarchy_type" IS 'the type of hierarchy level, e.g., "chapter", "subchapter", "part", "subpart", "section", "appendix"';
COMMENT ON COLUMN "title_details"."hierarchy_level" IS 'the level of the hierarchy, starting from 1 for the highest level';
COMMENT ON COLUMN "title_details"."is_leaf_node" IS 'true if this item has no children (is a terminal/leaf node in the hierarchy), false if it has child elements';
COMMENT ON COLUMN "title_details"."reserved" IS 'true if this hierarchy element is reserved (not currently in use), false if it contains active regulations';
COMMENT ON COLUMN "title_details"."order_id" IS 'the order of the item within its hierarchy level, used for sorting';
