# introduction to the vectorizing principle

This repo is used to illustrate the vectorization principle in a tutorial. In data science applications,
large amounts of data are processed, and dynamically typed and interpreted languages like python, R.
or Matlab are often used to perform a job in just a few lines based on a highly dynamic library universe.
The heavy lifting is done within those libraries written in C, C++ or Fortran. Thus the user API to those libraries
must be based on handing around large amounts of data instead of single values.
This is the basis of the vectorization principle.

This tutorial was created for a CEOI workshop in August 2023 and might not be kept up-to-date.

Disclaimer: The term vectorization is also used for talking about using SIMD based instruction level parallelism
provided by CPUs. Here, we talk about vectorization as a library design pattern for structural data transformation
code – applying operations to vectors instead of scalars.

## 🌏  Open in the Cloud 

Click any of the buttons below to start a new development environment to demo or contribute to the codebase without having to install anything on your machine:

[![Open in VS Code](https://img.shields.io/badge/Open%20in-VS%20Code-blue?logo=visualstudiocode)](https://vscode.dev/github/Quantco/vectorization-tutorial)
[![Open in Glitch](https://img.shields.io/badge/Open%20in-Glitch-blue?logo=glitch)](https://glitch.com/edit/#!/import/github/Quantco/vectorization-tutorial)
[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/Quantco/vectorization-tutorial)
[![Open in StackBlitz](https://developer.stackblitz.com/img/open_in_stackblitz.svg)](https://stackblitz.com/github/Quantco/vectorization-tutorial)
[![Edit in Codesandbox](https://codesandbox.io/static/img/play-codesandbox.svg)](https://codesandbox.io/s/github/Quantco/vectorization-tutorial)
[![Open in Repl.it](https://replit.com/badge/github/withastro/astro)](https://replit.com/github/Quantco/vectorization-tutorial)
[![Open in Codeanywhere](https://codeanywhere.com/img/open-in-codeanywhere-btn.svg)](https://app.codeanywhere.com/#https://github.com/Quantco/vectorization-tutorial)
[![Open in Gitpod](https://gitpod.io/button/open-in-gitpod.svg)](https://gitpod.io/#https://github.com/Quantco/vectorization-tutorial)

## Try it yourself in a GitHub Codespace (VS Code)

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/Quantco/vectorization-tutorial/)

Default options to run a small codespace are perfectly fine (free-tier is provided by GitHub).

The first time you enter the codespace, it takes some time to set up the environment and install all dependencies
(the script doing that might only run ~1min after starting the codespace).
Please, take a coffee break and just wait...

After environment is installed, it should be possible to open jupyter notebooks (*.ipynb files) in VS Code.
Install extensions in case VS Code is asking, and choose python environment `vectorization`.

To run code in the terminal, activate the environment via

```bash
pixi shell
```

## Try it yourself on your local machine

### Setting up the environment for running the python files and jupyter notebooks in this repository

Follow https://pixi.sh to install pixi.
Then run the following commands to create a new environment and install the required packages:

```bash
pixi shell
```

## Table of Contents:

- [vectorization01.ipynb](vectorization01.ipynb): moving the loop into the library
- [vectorization02.ipynb](vectorization02.ipynb): vectorized translation of conditional statements
- [vectorization03.ipynb](vectorization03.ipynb): a slightly more complex example
- [vectorization04.ipynb](vectorization04.ipynb): defining a data pipeline
- [vectorization05a.ipynb](vectorization05a.ipynb): generating SQL and dataframe transformation code with one syntax
- [vectorization05b.ipynb](vectorization05b.ipynb): generating SQL and dataframe transformation code with one syntax - an example pipeline
- [vectorization06.ipynb](vectorization06.ipynb): many ways to describe data transformations in python
- [vectorization07.ipynb](vectorization07.ipynb): aggregation functions
- [vectorization08.ipynb](vectorization08.ipynb): window functions
