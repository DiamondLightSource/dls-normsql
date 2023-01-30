.. # ********** Please don't edit this file!
.. # ********** It has been generated automatically by dae_devops version 0.4.0.
.. # ********** For repository_name dls-normsql

Developing
=======================================================================

If you plan to make change to the code in this repository, you can use the steps below.

Clone the repository::

    $ git clone https://gitlab.diamond.ac.uk/kbp43231/dls-normsql.git

It is recommended that you install into a virtual environment so this
installation will not interfere with any existing Python software.
Make sure to have at least python version 3.8 then::

    $ python3 -m venv /scratch/$USER/myvenv
    $ source /scratch/$USER/myvenv/bin/activate
    $ pip install --upgrade pip

Install the package in edit mode which will also install all its dependencies::

    $ cd dls-normsql
    $ export PIP_FIND_LINKS=/dls_sw/apps/bxflow/artifacts
    $ pip install -e .[dev]

Now you may begin modifying the code.

|

If you plan to modify the docs, you will need to::

    $ pip install -e .[docs]

    


.. # dae_devops_fingerprint 07f1e374bc286bfd64edca93fbfbec4b
