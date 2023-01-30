dls-normsql
=======================================================================

Normalized API over various sql libraries.

Intended advantages:

- common connection and set of sql commands
- reusable library for consistency across multiple projects

Installation
-----------------------------------------------------------------------
::

    pip install git+https://gitlab.diamond.ac.uk/kbp43231/dls-normsql.git 

    dls-normsql --version

Documentation
-----------------------------------------------------------------------

See https://www.cs.diamond.ac.uk/dls-normsql for more detailed documentation.

Building and viewing the documents locally::

    git clone git+https://gitlab.diamond.ac.uk/kbp43231/dls-normsql.git 
    cd dls-normsql
    virtualenv /scratch/$USER/venv/dls-normsql
    source /scratch/$USER/venv/dls-normsql/bin/activate 
    pip install -e .[dev]
    make -f .dls-normsql/Makefile validate_docs
    browse to file:///scratch/$USER/venvs/dls-normsql/build/html/index.html

Topics for further documentation:

- TODO list of improvements
- change log


..
    Anything below this line is used when viewing README.rst and will be replaced
    when included in index.rst

