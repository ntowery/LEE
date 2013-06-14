/* load UDFs */
/* ********************************************************************************************** */
REGISTER 's3n://netflix-dataoven-prod-users/DSE/etl_code/experimentation/cust_allocs/udfs.py' using jython as udfs;
REGISTER 's3n://netflix-dataoven-prod-users/DSE/etl_code/experimentation/cust_allocs/testing.py' using jython as testing;
REGISTER 's3://netflix-dataoven-prod/genie/jars/aegisthus.jar';
REGISTER 's3://netflix-dataoven-prod/genie/jars/dse_pig.jar';
/* load and format lookups */
/* ********************************************************************************************** */
/* ab subgroups */
/* ab_subgroups_enum_raw = LOAD '$ab_enums' USING com.netflix.pig.load.AegisthusBagLoader('true'); */
ab_subgroups_enum_raw = LOAD 's3n://nflx.dse.test.datadrop/aegisthus/prod/full/us-east-1/20130608/abcassandra/abadmin/ab_enums.gz' USING com.netflix.pig.load.AegisthusBagLoader('true');

ab_exp_allocation_plan_d_hive = LOAD 'prodhive.lsagi.lsg_ab_exp_allocation_plan_d_final' USING DseStorage() AS
(test_id:int,
 allocation_plan_id:chararray, 
 precent_wanted:chararray, 
 start_date:chararray, 
 end_date:chararray,
 category_dd:chararray, 
 subcategory_id:int, 
 subcategory_desc:chararray,
 plan_type:chararray, 
 batch_status:chararray,
 country_iso_code:chararray,
 cells:chararray,
 created_date:chararray,
 expected_customer_count:int,
 updated_by_name:chararray,
 start_time_range:int,
 end_time_range:chararray,
 source_test_id:int,
 source_cells_list:chararray,
 destination_cell:int,
 operation:chararray,
 allocation_state:chararray);

ab_subgroups_enum_cols = FOREACH ab_subgroups_enum_raw GENERATE (chararray)key, columns.(name, value) AS namevalues;
ab_subgroups_enum_flattened = FOREACH ab_subgroups_enum_cols GENERATE key, flatten(namevalues);
ab_subgroups_enum_flatteend_cleansed = FOREACH ab_subgroups_enum_flattened GENERATE flatten(STRSPLIT(TRIM(namevalues::name), ':', 2)) AS (key:chararray, name:chararray), value as value;
SPLIT ab_subgroups_enum_flatteend_cleansed INTO
ab_subgroups_enum_name IF name=='name',
ab_subgroups_enum_visible IF name=='visible';
ab_subgroups_enum_name_typed = FOREACH ab_subgroups_enum_name GENERATE 
key
, (chararray)value;

ab_subgroups_enum_visible_typed = FOREACH ab_subgroups_enum_visible GENERATE 
key
, (chararray)value;
ab_subgroups_lkp_join = JOIN ab_subgroups_enum_name_typed BY (key) LEFT, ab_subgroups_enum_visible_typed BY (key);
ab_subgroups_lkp = FOREACH ab_subgroups_lkp_join GENERATE 
ab_subgroups_enum_name_typed::key AS key
, ab_subgroups_enum_name_typed::value AS subgroup_name
, ab_subgroups_enum_visible_typed::value AS isVisible;



/* get today's file */
/* ********************************************************************************************** */
/* load today's full files */
/* t_full = LOAD '$today_full' USING com.netflix.pig.load.AegisthusBagLoader('true'); */
t_full = LOAD 's3n://nflx.dse.test.datadrop/aegisthus/prod/full/us-east-1/20130608/abcassandra/abtests/abdefinitions.gz' USING com.netflix.pig.load.AegisthusBagLoader('true');

/* only get key and name/values (drop epoch timestamps) */
t_cols = FOREACH t_full GENERATE 
(chararray)key, columns.(name, value) AS namevalues;

/* clean incremental records */
flattened = FOREACH t_cols GENERATE 
key AS key
, flatten(namevalues);

flattened_cleansed = FOREACH flattened GENERATE 
key
, TRIM(namevalues::name) AS name
, namevalues::value AS value;

/* split into test, cell, and plan relations */
SPLIT flattened_cleansed INTO
tests IF INDEXOF(name, 'b', 0)==0,
cells IF INDEXOF(name, 'c', 0)==0,
allocation_plans IF INDEXOF(name, 'p', 0)==0 OR INDEXOF(name, 'bulkPlan',0)==0;

/* clean tests, cells, and allocation plans column names */
tests_cleansed = FOREACH tests GENERATE key, REPLACE(name, 'b   d   ', '') AS name, value;

cells_cleansed = FOREACH cells GENERATE key, flatten(STRSPLIT(REPLACE(name, 'cell   ', ''), '   ', 2)) AS (cell_num:chararray, name:chararray), value;

split allocation_plans into 
batch_allocation_plans if INDEXOF(name,'bulkPlan') >= 0, 
regular_allocation_plans if INDEXOF(name,'bulkPlan') < 0;

batch_allocation_plans_cleansed = FOREACH batch_allocation_plans GENERATE key, flatten(STRSPLIT(REPLACE(name, 'bulkPlan', ''), '   ', 2)) AS (allocation_plan_id:chararray, name:chararray), value;
regular_allocation_plans_cleansed = FOREACH regular_allocation_plans GENERATE key, flatten(STRSPLIT(REPLACE(name, 'plan', ''), '   ', 2)) AS (allocation_plan_id:chararray, name:chararray), value;
allocation_plans_cleansed = union batch_allocation_plans_cleansed, regular_allocation_plans_cleansed;

/* split into individual columns and cast datatypes */
/* ********************************************************************************************** */
/* split into individual columns for tests */
SPLIT tests_cleansed INTO
tests_name IF name=='name'
,tests_owner IF name=='owner'
,tests_devOwner IF name=='devOwner'
,tests_description IF name=='description'
,tests_csDescription IF name=='csDescription'
,tests_creationDate IF name=='creationDate'
,test_updatedBy IF name=='updatedBy'
,tests_csVisible IF name=='csVisible'
,tests_saveAllocations IF name=='saveAllocations'
,tests_enabled IF name=='enabled'
,tests_defaultCell IF name=='defaultCell'
,tests_tags IF name=='tags';

/* cast column datatypes for tests */
tests_name_typed = FOREACH tests_name GENERATE 
key
, udfs.clean_bytearray(value) AS value;

tests_owner_typed = FOREACH tests_owner GENERATE 
key
, udfs.clean_bytearray(value) AS value;

tests_devOwner_typed = FOREACH tests_devOwner GENERATE 
key
, udfs.clean_bytearray(value) AS value;

tests_description_typed = FOREACH tests_description GENERATE 
key
, udfs.clean_bytearray(value) AS value;

tests_csDescription_typed = FOREACH tests_csDescription GENERATE 
key
, udfs.clean_bytearray(value) AS value;

tests_creationDate_typed = FOREACH tests_creationDate GENERATE 
key
, (long)value;

test_updatedBy_typed = FOREACH test_updatedBy GENERATE 
key
, udfs.clean_bytearray(value) AS value;

tests_csVisible_typed = FOREACH tests_csVisible GENERATE 
key
, (chararray)value;

tests_saveAllocations_typed = FOREACH tests_saveAllocations GENERATE 
key
, (chararray)value;

tests_enabled_typed = FOREACH tests_enabled GENERATE 
key
, (chararray)value;

tests_defaultCell_typed = FOREACH tests_defaultCell GENERATE 
key
, (int)value;

tests_tags_typed = FOREACH tests_tags GENERATE 
key
, udfs.clean_bytearray(value) AS value;

/* split into individual columns for cells */
SPLIT cells_cleansed INTO
cells_name IF name=='name',
cells_controlCell IF name=='controlCell',
cells_csDescription IF name=='csDescription';

/* cast column datatypes for cells */
cells_name_typed = FOREACH cells_name GENERATE 
key
, cell_num
, (chararray)value;

cells_controlCell_typed = FOREACH cells_controlCell GENERATE 
key
, cell_num
, (int)value;

cells_csDescription_typed = FOREACH cells_csDescription GENERATE 
key
, cell_num
, udfs.clean_bytearray(value) AS value;

/* split into individual columns for allocation plans */
SPLIT allocation_plans_cleansed INTO
allocation_plans_percentWanted IF name=='percentWanted',
allocation_plans_startDate IF name=='startDate',
allocation_plans_endDate IF name=='endDate',
allocation_plans_categoryId IF name=='categoryId',
allocation_plans_subcategoryId IF name=='subcategoryId',
allocation_plans_type IF name=='type',
allocation_plans_batchStatus IF name=='batchStatus',
allocation_plans_country IF name=='country',
allocation_plans_countryId IF name=='countryId',
allocation_plans_cells IF name=='cells',
allocation_plans_createdDate IF name=='createdDate',
allocation_plans_expected IF name=='expected',
allocation_plans_updatedBy IF name=='updatedBy',
allocation_plans_startTimeRange IF name=='startTimeRange',
allocation_plans_endTimeRange IF name=='endTimeRange',
allocation_plans_sourceTestId IF name=='sourceTestId',
allocation_plans_sourceCells IF name=='sourceCells',
allocation_plans_destCell IF name=='destCell',
allocation_plans_op IF name=='op',
allocation_plans_allocationState IF name=='allocationState'
;

/* cast column datatypes for allocation plans*/

allocation_plans_percentWanted_typed = FOREACH allocation_plans_percentWanted GENERATE 
key
, TRIM(allocation_plan_id) as allocation_plan_id
, (long)udfs.clean_bytearray2(value) AS value;

allocation_plans_startDate_typed = FOREACH allocation_plans_startDate GENERATE 
key
, TRIM(allocation_plan_id) as allocation_plan_id
, (long)udfs.clean_bytearray(value) AS value;

allocation_plans_endDate_typed = FOREACH allocation_plans_endDate GENERATE 
key
, TRIM(allocation_plan_id) as allocation_plan_id
, (long)udfs.clean_bytearray(value) AS value;

allocation_plans_categoryId_typed = FOREACH allocation_plans_categoryId GENERATE 
key
, TRIM(allocation_plan_id) as allocation_plan_id
, (chararray)udfs.clean_bytearray2(value) AS value;

allocation_plans_subcategoryId_typed = FOREACH allocation_plans_subcategoryId GENERATE 
key
, TRIM(allocation_plan_id) as allocation_plan_id
, (chararray)udfs.clean_bytearray2(value) AS value;

allocation_plans_type_typed = FOREACH allocation_plans_type GENERATE 
key
, TRIM(allocation_plan_id) as allocation_plan_id
, (chararray)udfs.clean_bytearray(value) AS value;

allocation_plans_batchStatus_typed = FOREACH allocation_plans_batchStatus GENERATE 
key
, TRIM(allocation_plan_id) as allocation_plan_id
, (chararray)udfs.clean_bytearray(value) AS value;

allocation_plans_country_typed = FOREACH allocation_plans_country GENERATE 
key
, TRIM(allocation_plan_id) as allocation_plan_id
, (chararray)udfs.clean_bytearray(value) AS value;

/*allocation_plans_countryId_typed = FOREACH allocation_plans_countryId GENERATE key, allocation_plan_id, (int)value;*/

allocation_plans_cells_typed = FOREACH allocation_plans_cells GENERATE 
key
, TRIM(allocation_plan_id) as allocation_plan_id
, (chararray)udfs.clean_bytearray(value) AS value;

allocation_plans_createdDate_typed = FOREACH allocation_plans_createdDate GENERATE 
key
, TRIM(allocation_plan_id) as allocation_plan_id
, (long)udfs.clean_bytearray(value) AS value;

allocation_plans_expected_typed = FOREACH allocation_plans_expected GENERATE 
key
, TRIM(allocation_plan_id) as allocation_plan_id
, (int)udfs.clean_bytearray(value) AS value;

allocation_plans_updatedBy_typed = FOREACH allocation_plans_updatedBy GENERATE 
key
, TRIM(allocation_plan_id) as allocation_plan_id
, (chararray)udfs.clean_bytearray(value) AS value;

allocation_plans_startTimeRange_typed = FOREACH allocation_plans_startTimeRange GENERATE 
key
, TRIM(allocation_plan_id) as allocation_plan_id
, (long)udfs.clean_bytearray(value) AS value;

allocation_plans_endTimeRange_typed = FOREACH allocation_plans_endTimeRange GENERATE 
key
, TRIM(allocation_plan_id) as allocation_plan_id
, (long)udfs.clean_bytearray(value) AS value;

allocation_plans_sourceTestId_typed = FOREACH allocation_plans_sourceTestId GENERATE 
key
, TRIM(allocation_plan_id) as allocation_plan_id
, (int)udfs.clean_bytearray(value) AS value;

allocation_plans_sourceCells_typed = FOREACH allocation_plans_sourceCells GENERATE 
key
, TRIM(allocation_plan_id) as allocation_plan_id
, (chararray)udfs.clean_bytearray(value) AS value;

allocation_plans_destCell_typed = FOREACH allocation_plans_destCell GENERATE 
key
, TRIM(allocation_plan_id) as allocation_plan_id
, (chararray)udfs.clean_bytearray2(value) AS value;

allocation_plans_op_typed = FOREACH allocation_plans_op GENERATE 
key
, TRIM(allocation_plan_id) as allocation_plan_id
, (chararray)udfs.clean_bytearray(value) AS value;

allocation_plans_allocationState_typed = FOREACH allocation_plans_allocationState GENERATE 
key
, TRIM(allocation_plan_id) as allocation_plan_id
, (chararray)udfs.clean_bytearray(value) AS value;

/* perform lookups/transforms */
/* ********************************************************************************************** */
allocation_plans_subcategory_temp = JOIN allocation_plans_subcategoryId_typed BY ((chararray)value) LEFT, ab_subgroups_lkp BY (key) USING 'replicated';

allocation_plans_subcategory = FOREACH allocation_plans_subcategory_temp GENERATE 
allocation_plans_subcategoryId_typed::key AS key
, allocation_plans_subcategoryId_typed::allocation_plan_id AS allocation_plan_id
, allocation_plans_subcategoryId_typed::value AS subcategoryId
, ab_subgroups_lkp::subgroup_name AS subcategoryDesc;

/* join columns back together */
/* ********************************************************************************************** */
/* tests */
tests_def_1 = JOIN tests_name_typed BY (key), tests_creationDate_typed BY (key), tests_enabled_typed BY (key), tests_defaultCell_typed BY (key);
tests_def_2 = JOIN tests_def_1 BY (tests_name_typed::key) LEFT, tests_owner_typed BY (key);
tests_def_3 = JOIN tests_def_2 BY (tests_def_1::tests_name_typed::key) LEFT, tests_devOwner_typed BY (key);
tests_def_4 = JOIN tests_def_3 BY (tests_def_2::tests_def_1::tests_name_typed::key) LEFT, tests_description_typed BY (key);
tests_def_5 = JOIN tests_def_4 BY (tests_def_3::tests_def_2::tests_def_1::tests_name_typed::key) LEFT, tests_tags_typed BY (key);

tests_defs = FOREACH tests_def_5 GENERATE 
tests_def_4::tests_def_3::tests_def_2::tests_def_1::tests_name_typed::key AS key
, tests_def_4::tests_def_3::tests_def_2::tests_def_1::tests_name_typed::value AS name
, tests_def_4::tests_def_3::tests_def_2::tests_def_1::tests_creationDate_typed::value AS creationDate
, tests_def_4::tests_def_3::tests_def_2::tests_def_1::tests_enabled_typed::value AS enabled
, tests_def_4::tests_def_3::tests_def_2::tests_def_1::tests_defaultCell_typed::value AS defaultCell
, tests_def_4::tests_def_3::tests_def_2::tests_owner_typed::value AS owner
, tests_def_4::tests_def_3::tests_devOwner_typed::value AS devOwner
, tests_def_4::tests_description_typed::value AS description
, tests_tags_typed::value AS tags;

--tests_defs = FOREACH tests_def_4 GENERATE tests_def_3::tests_def_2::tests_def_1::tests_name_typed::key AS key, tests_def_3::tests_def_2::tests_def_1::tests_name_typed::value AS name, tests_def_3::tests_def_2::tests_def_1::tests_creationDate_typed::value AS creationDate, tests_def_3::tests_def_2::tests_def_1::tests_enabled_typed::value AS enabled, tests_def_3::tests_def_2::tests_def_1::tests_defaultCell_typed::value AS defaultCell, tests_def_3::tests_def_2::tests_owner_typed::value AS owner, tests_def_3::tests_devOwner_typed::value AS devOwner, tests_description_typed::value AS description;

/* cells */
cells_def_1 = JOIN cells_name_typed BY (key, cell_num) LEFT, cells_controlCell_typed BY (key, cell_num);

cells_def_2 = JOIN cells_def_1 BY (cells_name_typed::key, cells_name_typed::cell_num) LEFT, cells_csDescription_typed BY (key, cell_num);

cells_def_3 = JOIN cells_def_2 BY (cells_def_1::cells_name_typed::key) LEFT, tests_defaultCell_typed BY (key);

cells_defs = FOREACH cells_def_3 GENERATE 
cells_def_2::cells_def_1::cells_name_typed::key AS key
, cells_def_2::cells_def_1::cells_name_typed::cell_num AS cell_num
, cells_def_2::cells_def_1::cells_name_typed::value AS name
, cells_def_2::cells_csDescription_typed::value AS csDescription
, (tests_defaultCell_typed::value==(int)cells_def_2::cells_def_1::cells_name_typed::cell_num?'Y':'N') AS isDefaultCell;

/* allocation plans */
allocation_plans_def_1 = JOIN allocation_plans_percentWanted_typed BY (key, allocation_plan_id) LEFT, allocation_plans_startDate_typed BY (key, allocation_plan_id) using 'replicated';

allocation_plans_def_2 = JOIN allocation_plans_def_1 BY (allocation_plans_percentWanted_typed::key, allocation_plans_percentWanted_typed::allocation_plan_id) LEFT, allocation_plans_endDate_typed BY (key, allocation_plan_id) using 'replicated';

allocation_plans_def_3 = JOIN allocation_plans_def_2 BY (allocation_plans_def_1::allocation_plans_percentWanted_typed::key, allocation_plans_def_1::allocation_plans_percentWanted_typed::allocation_plan_id) LEFT, allocation_plans_categoryId_typed BY (key, allocation_plan_id) using 'replicated';

allocation_plans_def_4 = JOIN allocation_plans_def_3 BY (allocation_plans_def_2::allocation_plans_def_1::allocation_plans_percentWanted_typed::key, allocation_plans_def_2::allocation_plans_def_1::allocation_plans_percentWanted_typed::allocation_plan_id) LEFT, allocation_plans_subcategory BY (key, allocation_plan_id) using 'replicated';

allocation_plans_def_5 = JOIN allocation_plans_def_4 BY (allocation_plans_def_3::allocation_plans_def_2::allocation_plans_def_1::allocation_plans_percentWanted_typed::key, allocation_plans_def_3::allocation_plans_def_2::allocation_plans_def_1::allocation_plans_percentWanted_typed::allocation_plan_id) LEFT, allocation_plans_type_typed BY (key, allocation_plan_id) using 'replicated';

allocation_plans_def_6 = JOIN allocation_plans_def_5 BY (allocation_plans_def_4::allocation_plans_def_3::allocation_plans_def_2::allocation_plans_def_1::allocation_plans_percentWanted_typed::key, allocation_plans_def_4::allocation_plans_def_3::allocation_plans_def_2::allocation_plans_def_1::allocation_plans_percentWanted_typed::allocation_plan_id) LEFT, allocation_plans_batchStatus_typed BY (key, allocation_plan_id) using 'replicated';

allocation_plans_def_7 = JOIN allocation_plans_def_6 BY (allocation_plans_def_5::allocation_plans_def_4::allocation_plans_def_3::allocation_plans_def_2::allocation_plans_def_1::allocation_plans_percentWanted_typed::key, allocation_plans_def_5::allocation_plans_def_4::allocation_plans_def_3::allocation_plans_def_2::allocation_plans_def_1::allocation_plans_percentWanted_typed::allocation_plan_id) LEFT, allocation_plans_country_typed BY (key, allocation_plan_id) using 'replicated';

allocation_plans_def_8 = JOIN allocation_plans_def_7 BY (allocation_plans_def_6::allocation_plans_def_5::allocation_plans_def_4::allocation_plans_def_3::allocation_plans_def_2::allocation_plans_def_1::allocation_plans_percentWanted_typed::key, allocation_plans_def_6::allocation_plans_def_5::allocation_plans_def_4::allocation_plans_def_3::allocation_plans_def_2::allocation_plans_def_1::allocation_plans_percentWanted_typed::allocation_plan_id) LEFT, allocation_plans_cells_typed BY (key, allocation_plan_id) using 'replicated';

allocation_plans_def_9 = JOIN allocation_plans_def_8 BY (allocation_plans_def_7::allocation_plans_def_6::allocation_plans_def_5::allocation_plans_def_4::allocation_plans_def_3::allocation_plans_def_2::allocation_plans_def_1::allocation_plans_percentWanted_typed::key, allocation_plans_def_6::allocation_plans_def_5::allocation_plans_def_4::allocation_plans_def_3::allocation_plans_def_2::allocation_plans_def_1::allocation_plans_percentWanted_typed::allocation_plan_id) LEFT, allocation_plans_createdDate_typed BY (key, allocation_plan_id) using 'replicated';

allocation_plans_def_10 = JOIN allocation_plans_def_9 BY (allocation_plans_def_8::allocation_plans_def_7::allocation_plans_def_6::allocation_plans_def_5::allocation_plans_def_4::allocation_plans_def_3::allocation_plans_def_2::allocation_plans_def_1::allocation_plans_percentWanted_typed::key, allocation_plans_def_6::allocation_plans_def_5::allocation_plans_def_4::allocation_plans_def_3::allocation_plans_def_2::allocation_plans_def_1::allocation_plans_percentWanted_typed::allocation_plan_id) LEFT, allocation_plans_expected_typed BY (key, allocation_plan_id) using 'replicated';

allocation_plans_def_11 = JOIN allocation_plans_def_10 BY (allocation_plans_def_9::allocation_plans_def_8::allocation_plans_def_7::allocation_plans_def_6::allocation_plans_def_5::allocation_plans_def_4::allocation_plans_def_3::allocation_plans_def_2::allocation_plans_def_1::allocation_plans_percentWanted_typed::key, allocation_plans_def_6::allocation_plans_def_5::allocation_plans_def_4::allocation_plans_def_3::allocation_plans_def_2::allocation_plans_def_1::allocation_plans_percentWanted_typed::allocation_plan_id) LEFT, allocation_plans_updatedBy_typed BY (key, allocation_plan_id) using 'replicated';

allocation_plans_def_12 = JOIN allocation_plans_def_11 BY (allocation_plans_def_10::allocation_plans_def_9::allocation_plans_def_8::allocation_plans_def_7::allocation_plans_def_6::allocation_plans_def_5::allocation_plans_def_4::allocation_plans_def_3::allocation_plans_def_2::allocation_plans_def_1::allocation_plans_percentWanted_typed::key, allocation_plans_def_6::allocation_plans_def_5::allocation_plans_def_4::allocation_plans_def_3::allocation_plans_def_2::allocation_plans_def_1::allocation_plans_percentWanted_typed::allocation_plan_id) LEFT, allocation_plans_startTimeRange_typed BY (key, allocation_plan_id) using 'replicated';
  
allocation_plans_def_13 = JOIN allocation_plans_def_12 BY (allocation_plans_def_11::allocation_plans_def_10::allocation_plans_def_9::allocation_plans_def_8::allocation_plans_def_7::allocation_plans_def_6::allocation_plans_def_5::allocation_plans_def_4::allocation_plans_def_3::allocation_plans_def_2::allocation_plans_def_1::allocation_plans_percentWanted_typed::key, allocation_plans_def_6::allocation_plans_def_5::allocation_plans_def_4::allocation_plans_def_3::allocation_plans_def_2::allocation_plans_def_1::allocation_plans_percentWanted_typed::allocation_plan_id) LEFT, allocation_plans_endTimeRange_typed BY (key, allocation_plan_id) using 'replicated';

allocation_plans_def_14 = JOIN allocation_plans_def_13 BY (allocation_plans_def_12::allocation_plans_def_11::allocation_plans_def_10::allocation_plans_def_9::allocation_plans_def_8::allocation_plans_def_7::allocation_plans_def_6::allocation_plans_def_5::allocation_plans_def_4::allocation_plans_def_3::allocation_plans_def_2::allocation_plans_def_1::allocation_plans_percentWanted_typed::key, allocation_plans_def_6::allocation_plans_def_5::allocation_plans_def_4::allocation_plans_def_3::allocation_plans_def_2::allocation_plans_def_1::allocation_plans_percentWanted_typed::allocation_plan_id) LEFT, allocation_plans_sourceTestId_typed BY (key, allocation_plan_id) using 'replicated';
 
allocation_plans_def_15 = JOIN allocation_plans_def_14 BY (allocation_plans_def_13::allocation_plans_def_12::allocation_plans_def_11::allocation_plans_def_10::allocation_plans_def_9::allocation_plans_def_8::allocation_plans_def_7::allocation_plans_def_6::allocation_plans_def_5::allocation_plans_def_4::allocation_plans_def_3::allocation_plans_def_2::allocation_plans_def_1::allocation_plans_percentWanted_typed::key, allocation_plans_def_6::allocation_plans_def_5::allocation_plans_def_4::allocation_plans_def_3::allocation_plans_def_2::allocation_plans_def_1::allocation_plans_percentWanted_typed::allocation_plan_id) LEFT, allocation_plans_sourceCells_typed BY (key, allocation_plan_id) using 'replicated';
 
allocation_plans_def_16 = JOIN allocation_plans_def_15 BY (allocation_plans_def_14::allocation_plans_def_13::allocation_plans_def_12::allocation_plans_def_11::allocation_plans_def_10::allocation_plans_def_9::allocation_plans_def_8::allocation_plans_def_7::allocation_plans_def_6::allocation_plans_def_5::allocation_plans_def_4::allocation_plans_def_3::allocation_plans_def_2::allocation_plans_def_1::allocation_plans_percentWanted_typed::key, allocation_plans_def_6::allocation_plans_def_5::allocation_plans_def_4::allocation_plans_def_3::allocation_plans_def_2::allocation_plans_def_1::allocation_plans_percentWanted_typed::allocation_plan_id) LEFT, allocation_plans_destCell_typed BY (key, allocation_plan_id) using 'replicated';

allocation_plans_def_17 = JOIN allocation_plans_def_16 BY (allocation_plans_def_15::allocation_plans_def_14::allocation_plans_def_13::allocation_plans_def_12::allocation_plans_def_11::allocation_plans_def_10::allocation_plans_def_9::allocation_plans_def_8::allocation_plans_def_7::allocation_plans_def_6::allocation_plans_def_5::allocation_plans_def_4::allocation_plans_def_3::allocation_plans_def_2::allocation_plans_def_1::allocation_plans_percentWanted_typed::key, allocation_plans_def_6::allocation_plans_def_5::allocation_plans_def_4::allocation_plans_def_3::allocation_plans_def_2::allocation_plans_def_1::allocation_plans_percentWanted_typed::allocation_plan_id) LEFT, allocation_plans_op_typed BY (key, allocation_plan_id) using 'replicated';
 
allocation_plans_def_18 = JOIN allocation_plans_def_17 BY (allocation_plans_def_16::allocation_plans_def_15::allocation_plans_def_14::allocation_plans_def_13::allocation_plans_def_12::allocation_plans_def_11::allocation_plans_def_10::allocation_plans_def_9::allocation_plans_def_8::allocation_plans_def_7::allocation_plans_def_6::allocation_plans_def_5::allocation_plans_def_4::allocation_plans_def_3::allocation_plans_def_2::allocation_plans_def_1::allocation_plans_percentWanted_typed::key, allocation_plans_def_6::allocation_plans_def_5::allocation_plans_def_4::allocation_plans_def_3::allocation_plans_def_2::allocation_plans_def_1::allocation_plans_percentWanted_typed::allocation_plan_id) LEFT, allocation_plans_allocationState_typed BY (key, allocation_plan_id) using 'replicated';

allocation_plans_defs = FOREACH allocation_plans_def_18 GENERATE 
allocation_plans_def_17::allocation_plans_def_16::allocation_plans_def_15::allocation_plans_def_14::allocation_plans_def_13::allocation_plans_def_12::allocation_plans_def_11::allocation_plans_def_10::allocation_plans_def_9::allocation_plans_def_8::allocation_plans_def_7::allocation_plans_def_6::allocation_plans_def_5::allocation_plans_def_4::allocation_plans_def_3::allocation_plans_def_2::allocation_plans_def_1::allocation_plans_percentWanted_typed::key AS key
, allocation_plans_def_17::allocation_plans_def_16::allocation_plans_def_15::allocation_plans_def_14::allocation_plans_def_13::allocation_plans_def_12::allocation_plans_def_11::allocation_plans_def_10::allocation_plans_def_9::allocation_plans_def_8::allocation_plans_def_7::allocation_plans_def_6::allocation_plans_def_5::allocation_plans_def_4::allocation_plans_def_3::allocation_plans_def_2::allocation_plans_def_1::allocation_plans_percentWanted_typed::allocation_plan_id AS allocation_plan_id
, allocation_plans_def_17::allocation_plans_def_16::allocation_plans_def_15::allocation_plans_def_14::allocation_plans_def_13::allocation_plans_def_12::allocation_plans_def_11::allocation_plans_def_10::allocation_plans_def_9::allocation_plans_def_8::allocation_plans_def_7::allocation_plans_def_6::allocation_plans_def_5::allocation_plans_def_4::allocation_plans_def_3::allocation_plans_def_2::allocation_plans_def_1::allocation_plans_percentWanted_typed::value AS precentWanted
, allocation_plans_def_17::allocation_plans_def_16::allocation_plans_def_15::allocation_plans_def_14::allocation_plans_def_13::allocation_plans_def_12::allocation_plans_def_11::allocation_plans_def_10::allocation_plans_def_9::allocation_plans_def_8::allocation_plans_def_7::allocation_plans_def_6::allocation_plans_def_5::allocation_plans_def_4::allocation_plans_def_3::allocation_plans_def_2::allocation_plans_def_1::allocation_plans_startDate_typed::value AS startDate
, allocation_plans_def_17::allocation_plans_def_16::allocation_plans_def_15::allocation_plans_def_14::allocation_plans_def_13::allocation_plans_def_12::allocation_plans_def_11::allocation_plans_def_10::allocation_plans_def_9::allocation_plans_def_8::allocation_plans_def_7::allocation_plans_def_6::allocation_plans_def_5::allocation_plans_def_4::allocation_plans_def_3::allocation_plans_def_2::allocation_plans_endDate_typed::value AS endDate
, allocation_plans_def_17::allocation_plans_def_16::allocation_plans_def_15::allocation_plans_def_14::allocation_plans_def_13::allocation_plans_def_12::allocation_plans_def_11::allocation_plans_def_10::allocation_plans_def_9::allocation_plans_def_8::allocation_plans_def_7::allocation_plans_def_6::allocation_plans_def_5::allocation_plans_def_4::allocation_plans_def_3::allocation_plans_categoryId_typed::value AS categoryId
, allocation_plans_def_17::allocation_plans_def_16::allocation_plans_def_15::allocation_plans_def_14::allocation_plans_def_13::allocation_plans_def_12::allocation_plans_def_11::allocation_plans_def_10::allocation_plans_def_9::allocation_plans_def_8::allocation_plans_def_7::allocation_plans_def_6::allocation_plans_def_5::allocation_plans_def_4::allocation_plans_subcategory::subcategoryId AS subcategoryId
, allocation_plans_def_17::allocation_plans_def_16::allocation_plans_def_15::allocation_plans_def_14::allocation_plans_def_13::allocation_plans_def_12::allocation_plans_def_11::allocation_plans_def_10::allocation_plans_def_9::allocation_plans_def_8::allocation_plans_def_7::allocation_plans_def_6::allocation_plans_def_5::allocation_plans_def_4::allocation_plans_subcategory::subcategoryDesc AS subcategoryDesc
, allocation_plans_def_17::allocation_plans_def_16::allocation_plans_def_15::allocation_plans_def_14::allocation_plans_def_13::allocation_plans_def_12::allocation_plans_def_11::allocation_plans_def_10::allocation_plans_def_9::allocation_plans_def_8::allocation_plans_def_7::allocation_plans_def_6::allocation_plans_def_5::allocation_plans_type_typed::value AS type
, allocation_plans_def_17::allocation_plans_def_16::allocation_plans_def_15::allocation_plans_def_14::allocation_plans_def_13::allocation_plans_def_12::allocation_plans_def_11::allocation_plans_def_10::allocation_plans_def_9::allocation_plans_def_8::allocation_plans_def_7::allocation_plans_def_6::allocation_plans_batchStatus_typed::value AS batchStatus
, allocation_plans_def_17::allocation_plans_def_16::allocation_plans_def_15::allocation_plans_def_14::allocation_plans_def_13::allocation_plans_def_12::allocation_plans_def_11::allocation_plans_def_10::allocation_plans_def_9::allocation_plans_def_8::allocation_plans_def_7::allocation_plans_country_typed::value AS country
, allocation_plans_def_17::allocation_plans_def_16::allocation_plans_def_15::allocation_plans_def_14::allocation_plans_def_13::allocation_plans_def_12::allocation_plans_def_11::allocation_plans_def_10::allocation_plans_def_9::allocation_plans_def_8::allocation_plans_cells_typed::value AS cells
, allocation_plans_def_17::allocation_plans_def_16::allocation_plans_def_15::allocation_plans_def_14::allocation_plans_def_13::allocation_plans_def_12::allocation_plans_def_11::allocation_plans_def_10::allocation_plans_def_9::allocation_plans_createdDate_typed::value AS createdDate
, allocation_plans_def_17::allocation_plans_def_16::allocation_plans_def_15::allocation_plans_def_14::allocation_plans_def_13::allocation_plans_def_12::allocation_plans_def_11::allocation_plans_def_10::allocation_plans_expected_typed::value AS expected  
, allocation_plans_def_17::allocation_plans_def_16::allocation_plans_def_15::allocation_plans_def_14::allocation_plans_def_13::allocation_plans_def_12::allocation_plans_def_11::allocation_plans_updatedBy_typed::value AS updatedBy 
, allocation_plans_def_17::allocation_plans_def_16::allocation_plans_def_15::allocation_plans_def_14::allocation_plans_def_13::allocation_plans_def_12::allocation_plans_startTimeRange_typed::value AS startTimeRange 
, allocation_plans_def_17::allocation_plans_def_16::allocation_plans_def_15::allocation_plans_def_14::allocation_plans_def_13::allocation_plans_endTimeRange_typed::value AS endTimeRange 
, allocation_plans_def_17::allocation_plans_def_16::allocation_plans_def_15::allocation_plans_def_14::allocation_plans_sourceTestId_typed::value AS sourceTestId 
, allocation_plans_def_17::allocation_plans_def_16::allocation_plans_def_15::allocation_plans_sourceCells_typed::value AS sourceCells 
, allocation_plans_def_17::allocation_plans_def_16::allocation_plans_destCell_typed::value AS destCell 
, allocation_plans_def_17::allocation_plans_op_typed::value AS op 
, allocation_plans_allocationState_typed::value AS allocationState 
;

tests_tags_inc_split = FOREACH tests_defs GENERATE 
key
, flatten(STRSPLIT(tags, ',', 100)) AS (tag1:chararray, tag2:chararray, tag3:chararray, tag4:chararray, tag5:chararray, tag6:chararray, tag7:chararray, tag8:chararray, tag9:chararray);

tests_tags_inc_bag = FOREACH tests_tags_inc_split GENERATE 
key
, TOBAG(tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag8, tag9) AS bag_o_tags;

tests_tags_inc_norm = FOREACH tests_tags_inc_bag GENERATE 
key
, flatten(bag_o_tags) AS tag:chararray;

/* not writing this out anymore-is it needed? */
tests_tags_inc = DISTINCT tests_tags_inc_norm;

tests_defs_hive = FOREACH tests_defs GENERATE 
key
, name
, (creationDate is null ? 0 : testing.epoch_to_utc((long)creationDate/1000)) as creationDate
, enabled
, defaultCell
, owner
, devOwner
, description
, tags
;

cells_defs_hive = FOREACH cells_defs GENERATE 
key
, cell_num
, name
, csDescription
, isDefaultCell
;

allocation_plans_defs_hive = FOREACH allocation_plans_defs GENERATE 
key
, REPLACE(allocation_plan_id,'\\\$','') as allocation_plan_id 
, precentWanted
, (startDate is null ? 0 : testing.epoch_to_utc((long)startDate/1000)) as start_date
, (endDate is null ? 0 : testing.epoch_to_utc((long)endDate/1000)) as end_date
, categoryId
, subcategoryId
, subcategoryDesc
, type
, batchStatus
, country
, cells
, (createdDate is null ? 0 : testing.epoch_to_utc((long)createdDate/1000)) as created_date
, expected  
, updatedBy 
, (startTimeRange is null ? 0 : testing.epoch_to_utc((long)startTimeRange/1000)) as start_time_range
, (endTimeRange is null ? 0 : testing.epoch_to_utc((long)endTimeRange/1000)) as end_time_range
, sourceTestId as source_test_id
, sourceCells as source_cells
, destCell as destination_cell
, op as op
, allocationState as allocation_state
;

--Keep allocation plan history

full_allocation_plans = JOIN ab_exp_allocation_plan_d_hive BY (allocation_plan_id) FULL OUTER,
                             allocation_plans_defs_hive BY (allocation_plan_id);
                                
full_deduped_allocation_plan_d = FOREACH full_allocation_plans GENERATE
(allocation_plans_defs_hive::allocation_plan_id IS NULL?(chararray)ab_exp_allocation_plan_d_hive::test_id:(chararray)allocation_plans_defs_hive::key) AS test_id,
(allocation_plans_defs_hive::allocation_plan_id IS NULL?(chararray)ab_exp_allocation_plan_d_hive::allocation_plan_id:(chararray)allocation_plans_defs_hive::allocation_plan_id) AS allocation_plan_id,
(allocation_plans_defs_hive::allocation_plan_id IS NULL?(chararray)ab_exp_allocation_plan_d_hive::precent_wanted:(chararray)allocation_plans_defs_hive::precentWanted) AS precent_wanted,
(allocation_plans_defs_hive::allocation_plan_id IS NULL?(long)ab_exp_allocation_plan_d_hive::start_date:(long)allocation_plans_defs_hive::start_date) AS start_date,
(allocation_plans_defs_hive::allocation_plan_id IS NULL?(long)ab_exp_allocation_plan_d_hive::end_date:(long)allocation_plans_defs_hive::end_date) AS end_date,
(allocation_plans_defs_hive::allocation_plan_id IS NULL?(chararray)ab_exp_allocation_plan_d_hive::category_dd:(chararray)allocation_plans_defs_hive::categoryId) AS category_dd,
(allocation_plans_defs_hive::allocation_plan_id IS NULL?(int)ab_exp_allocation_plan_d_hive::subcategory_id:(int)allocation_plans_defs_hive::subcategoryId) AS subcategory_id,
(allocation_plans_defs_hive::allocation_plan_id IS NULL?(chararray)ab_exp_allocation_plan_d_hive::subcategory_desc:(chararray)allocation_plans_defs_hive::subcategoryDesc) AS subcategory_desc,
(allocation_plans_defs_hive::allocation_plan_id IS NULL?(chararray)ab_exp_allocation_plan_d_hive::plan_type:(chararray)allocation_plans_defs_hive::type) AS plan_type,
(allocation_plans_defs_hive::allocation_plan_id IS NULL?(chararray)ab_exp_allocation_plan_d_hive::batch_status:(chararray)allocation_plans_defs_hive::batchStatus) AS batch_status,
(allocation_plans_defs_hive::allocation_plan_id IS NULL?(chararray)ab_exp_allocation_plan_d_hive::country_iso_code:(chararray)allocation_plans_defs_hive::country) AS country_iso_code,
(allocation_plans_defs_hive::allocation_plan_id IS NULL?(chararray)ab_exp_allocation_plan_d_hive::cells:(chararray)allocation_plans_defs_hive::cells) AS cells,
(allocation_plans_defs_hive::allocation_plan_id IS NULL?(long)ab_exp_allocation_plan_d_hive::created_date:(long)allocation_plans_defs_hive::created_date) AS created_date,
(allocation_plans_defs_hive::allocation_plan_id IS NULL?(int)ab_exp_allocation_plan_d_hive::expected_customer_count:(int)allocation_plans_defs_hive::expected) AS expected_customer_count,
(allocation_plans_defs_hive::allocation_plan_id IS NULL?(chararray)ab_exp_allocation_plan_d_hive::updated_by_name:(chararray)allocation_plans_defs_hive::updatedBy) AS updated_by_name,
(allocation_plans_defs_hive::allocation_plan_id IS NULL?(long)ab_exp_allocation_plan_d_hive::start_time_range:(long)allocation_plans_defs_hive::start_time_range) AS start_time_range,
(allocation_plans_defs_hive::allocation_plan_id IS NULL?(long)ab_exp_allocation_plan_d_hive::end_time_range:(long)allocation_plans_defs_hive::end_time_range) AS end_time_range,
(allocation_plans_defs_hive::allocation_plan_id IS NULL?(int)ab_exp_allocation_plan_d_hive::source_test_id:(int)allocation_plans_defs_hive::source_test_id) AS source_test_id,
(allocation_plans_defs_hive::allocation_plan_id IS NULL?(chararray)ab_exp_allocation_plan_d_hive::source_cells_list:(chararray)allocation_plans_defs_hive::source_cells) AS source_cells_list,
(allocation_plans_defs_hive::allocation_plan_id IS NULL?(chararray)ab_exp_allocation_plan_d_hive::destination_cell:(chararray)allocation_plans_defs_hive::destination_cell) AS destination_cell,
(allocation_plans_defs_hive::allocation_plan_id IS NULL?(chararray)ab_exp_allocation_plan_d_hive::operation:(chararray)allocation_plans_defs_hive::op) AS operation,
(allocation_plans_defs_hive::allocation_plan_id IS NULL?(chararray)ab_exp_allocation_plan_d_hive::allocation_state:(chararray)allocation_plans_defs_hive::allocation_state) AS allocation_state;

/* store files */
/* ********************************************************************************************** */
/* STORE tests_tags_inc INTO '$output_datacenter_tests_tags' using PigStorage('\t'); */

--STORE tests_defs_hive INTO 'prodhive.dse.ab_exp_test_d' using DseBatchedStorage('gz'); 
--STORE cells_defs_hive INTO 'prodhive.dse.ab_exp_cell_d' using DseBatchedStorage('gz'); 
STORE full_deduped_allocation_plan_d INTO 'prodhive.dse.ab_exp_allocation_plan_d' using DseBatchedStorage('gz'); 
