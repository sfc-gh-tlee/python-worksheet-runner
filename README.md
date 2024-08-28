# Snowpark Worksheet Runner

This is a very rough script to help run clustering experiments.


## Install dependencies

These instructions are taken directly from here: [snowpark-python-template](https://github.com/Snowflake-Labs/snowpark-python-template)

Create and activate a conda environment using [Anaconda](https://conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#creating-an-environment-with-commands):

```bash
conda env create --file environment.yml
conda activate snowpark
```

### Configure IDE

#### VS Code

Press `Ctrl`+`Shift`+`P` to open the command palette, then select **Python: Select Interpreter** and select the **snowpark** interpreter under the **Conda** list.

#### PyCharm

Go to **File** > **Settings** > **Project** > **Python Interpreter** and select the snowpark interpreter.


## About the Script

The entry point is in `src/app.py`. The main function parses the command line argument and executes a setup or a type of clustering. There are functions that soley return sql text based on their options. The sql text is then put through the executor `run_queries` to execute them.

## Using This Script

To run, use

```bash
/opt/anaconda3/envs/snowpark/bin/python /Users/tlee/projects/snowpark-python-template/src/app.py <option>
```

To setup a table, change the `N_TABLE_ROWS` variable to the desired table size then use the `setup` option.

```bash
/opt/anaconda3/envs/snowpark/bin/python /Users/tlee/projects/snowpark-python-template/src/app.py setup
```

Subsequent runs will just clone the table that was setup to cluster. This can be done with the `cluster_auto` and `cluster_manual` options for either manual or automatic clustering. The variables at the top of `app.py` can be used to alter the parameters that are used for clustering.


## Sample Workflow For Background Clustering

1. Start and run my dev environment
2. Add the correct config to the `app.toml` and update the `CONFIG_NAME` variable in `app.py`
2. Setup a table with `/opt/anaconda3/envs/snowpark/bin/python /Users/tlee/projects/snowpark-python-template/src/app.py setup`
3. Set the desired parameters with the variables at the top of `app.py`
4. Run background clustering with `/opt/anaconda3/envs/snowpark/bin/python /Users/tlee/projects/snowpark-python-template/src/app.py cluster_auto`
5. Open http://snowflake.dev.local:53400/console#/monitoring/queries to see the queries that were run and also the background clustering statements

**Tip**: To obtain the sql queries to run without actually running them, you can comment out the execute line in the `run_queries` function. This lets you paste them manually into a worksheet to run at your own pace.
