# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "894291a9-f8d5-4ea0-8d4d-a41d3d9b7541",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# #### Initiate all test notebooks

# CELL ********************

%run nb_helper_functions_parent_caller

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_test_helper_functions_metadata

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_test_helper_functions_dataframe

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Run Nutter Tests Parallelly
# 
# Execute the nutter tests on a parallel basis.
# 


# CELL ********************

from runtime.runner import NutterFixtureParallelRunner
from notebookutils import mssparkutils

parallel_runner = NutterFixtureParallelRunner(num_of_workers=2)
parallel_runner.add_test_fixture(DataFrameTestFixture())
parallel_runner.add_test_fixture(MetadataTestFixture())

result = parallel_runner.execute()
print(result.to_string())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
