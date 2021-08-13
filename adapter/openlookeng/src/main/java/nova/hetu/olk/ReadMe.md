## How to Use openLooKeng adapter

We use an extension jar to support the decouple of OmniRuntime and OLK.

### how to build
you'd better build the project as fellow order:
1. mvn build omni-runtime 
2. mvn build hetu-core 
3. mvn build this adapter

### config extension execution planner

add the properties below to the config.properties :

// enable the extension execution planner
extension_execution_planner_enabled=true                  
// add the omni-openLooKeng-adapter jar path of extension execution planner 
extension_execution_planner_jar_path=file:///xxPath/omni-openLooKeng-adapter-1.2.0-SNAPSHOT.jar
// add the class path of the extension execution planner
extension_execution_planner_class_path=nova.hetu.olk.OmniLocalExecutionPlanner
// add the class path of the extension merge pages
extension_merge_pages_class_path=nova.hetu.olk.operator.filterandproject.OmniMergePages

### config the properties for scan operator

Since we don't have native scan operator yet, so we reuse the OLK scan operator. To support OmniVec, we implemented Omni
Block which extends from OLK Block. To use these OmniBlock, we will extends ColumnReader to generate Omni Block.

Now we support date/int/bigint/varchar/decimal types, and more types are under developing.

add the properties below to the etc/catalog/hive.properties to :
// enable the extension execution planner
extension_column_reader_jar_path=file:///xxPath/omni-openLooKeng-adapter-1.2.0-SNAPSHOT.jar
// add the class path of different types
extension_column_reader_type_long_classpath=nova.hetu.olk.reader.OmniLongColumnReader
extension_column_reader_type_int_classpath=nova.hetu.olk.reader.OmniIntColumnReader
extension_column_reader_type_date_classpath=nova.hetu.olk.reader.OmniDateColumnReader
extension_column_reader_type_slice_classpath=nova.hetu.olk.reader.OmniSliceColumnReader
extension_column_reader_type_decimal_classpath=nova.hetu.olk.reader.OmniDecimalColumnReader


### how to fall back to OLK
use the session below to fall back to OLK:
set session extension_execution_planner_enabled=false;
set session hive.extension_execution_planner_enabled=false;