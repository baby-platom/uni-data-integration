# uni-data-integration
The repository contains the code of a Data Integration project. The data set named "Pizza Place Sales" is taken from Kaggle and located at https://www.kaggle.com/datasets/mysarahmadbhat/pizza-place-sales/data.

## Table of Contents
1. [Overview](#overview)
2. [Installation](#installation)
3. [Usage](#usage)

## Overview
There are two root folders: `app` and `data`.
1. `app` contains the project's code
2. `data` keeps the used dataset.

The major goal of the project is to Extract, Transform, Load, and Analyze the data using `Apache Spark`. Since `Python` language is used, the `pyspark` package is leveraged for using the API of Spark. 

## Installation
The required Python version is `3.12.7`

1. Clone the repo
```sh
git clone https://github.com/baby-platom/uni-data-integration.git
```

2. Make sure that you have the Java Runtime Environment installed, you can do so by running `java -version`. It's crucial, since Spark relies on Java

3. Create virtual environment and install the dependencies
```sh
python -m venv venv
source venv/bin/activate

pip install -r requirements.txt
```

Optionally: 
- Use [uv](https://docs.astral.sh/uv/) for dependencies management
- Use [ruff](https://docs.astral.sh/ruff/) as a linter and code formatter

## Usage
Run the main script
```sh
python -m app.main
```
