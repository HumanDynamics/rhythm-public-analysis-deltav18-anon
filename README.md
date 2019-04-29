rhythm-private-analysis-deltav18
==============================

DeltaV 2018 analysis

Project Organization
------------

    ├── LICENSE
    ├── Makefile           <- Makefile with commands like `make data` or `make train`
    ├── README.md          <- The top-level README for developers using this project.
    ├── data
    │   ├── external       <- Data from third party sources.
    │   ├── interim        <- Intermediate data that has been transformed.
    │   ├── processed      <- The final, canonical data sets for modeling.
    │   └── raw            <- The original, immutable data dump.
    │
    ├── docs               <- A default Sphinx project; see sphinx-doc.org for details
    │
    ├── models             <- Trained and serialized models, model predictions, or model summaries
    │
    ├── notebooks          <- Jupyter notebooks. Naming convention is a number (for ordering),
    │                         the creator's initials, and a short `-` delimited description, e.g.
    │                         `1.0-jqp-initial-data-exploration`.
    │
    ├── references         <- Data dictionaries, manuals, and all other explanatory materials.
    │
    ├── reports            <- Generated analysis as HTML, PDF, LaTeX, etc.
    │   └── figures        <- Generated graphics and figures to be used in reporting
    │
    ├── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
    │                         generated with `pip freeze > requirements.txt`
    │
    ├── setup.py           <- makes project pip installable (pip install -e .) so src can be imported
    ├── src                <- Source code for use in this project.
    │   ├── __init__.py    <- Makes src a Python module
    │   │
    │   ├── data           <- Scripts to download or generate data
    │   │   └── make_dataset.py
    │   │
    │   ├── features       <- Scripts to turn raw data into features for modeling
    │   │   └── build_features.py
    │   │
    │   ├── models         <- Scripts to train models and then use trained models to make
    │   │   │                 predictions
    │   │   ├── predict_model.py
    │   │   └── train_model.py
    │   │
    │   └── visualization  <- Scripts to create exploratory and results oriented visualizations
    │       └── visualize.py
    │
    └── tox.ini            <- tox file with settings for running tox; see tox.testrun.org


--------

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>

# Notes for collaborators
## Setting up a working environemnt
1. Setup Python 2.7 (not 3.x)
2. Optional, but highly recommended - create a virtual python environment. See here - https://virtualenvwrapper.readthedocs.io/en/latest/ 
3. Setup the AWS command line interface (https://aws.amazon.com/cli/) . I'm using it to store the files separately from the code, and the data goes into a AWS bucket. Go easy on it though, it's on my personal account at the moment :) 
4. Clone this repository
5. Go into the repo's folder, and run "pip install -r requirements.txt". This will install the required python libraries in your environment
6. Run "make sync_from_s3" and it will pull some files to the data folder. If you did it before, do it again to get the latest files. 


This will setup everything you need in order to work. The next steps are:
* (optional) Run the shell script src/data/run_make_dataset.sh . This script does most of the pre-processing required for using the proximity data. For example, it replaces all the badge ID that you see in the scans with the matching member keys. We do this because in longer experiments, people might change badges, and the id can change. The reason why this script is optional is because I already ran it, and created the staging files (in data/interim). So when you sync files from the S3 bucket, you will already have this data
* Look at the example notebooks in notebooks/
* To run the notebook, you need to start jupyter-notebook in the notebook folder

# Data processing pipeline
The data processing pipeline prepares all the generic data stuctures (e.g. - member-to-beacon, member-to-member, etc), and it has several steps:
1. Group. Takes the raw data files collected by hubs and split the data into days. It enables the next step to run in parallel. This stage can take a long time
2. Process. Creates the main data structures. Runs in parallel (number of processes can be set in the config.py file)
3. Clean. Removes data that we don't need. For example, data before or after the experiment officialy took place, and data from inactive badges
4. Analysis. Adds generic highler level data structures. At the moment, it only computes the compliance tables, and uses this information to remove member-to-member interaction that took place when the badges were placed on the board.

Each of these stages has a .h5 file (HDF5 binary data file) associated with it that stores the resulting data structures.

