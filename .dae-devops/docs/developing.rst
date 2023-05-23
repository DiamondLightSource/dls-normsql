.. # ********** Please don't edit this file!
.. # ********** It has been generated automatically by dae_devops version 0.5.3.dev1+g36e9e1c.d20230523.
.. # ********** For repository_name dls-normsql

Developing
=======================================================================

If you plan to make change to the code in this repository, you can use the steps below.

Clone the repository::

    $ git clone https://gitlab.diamond.ac.uk/kbp43231/dls-normsql.git

It is recommended that you install into a virtual environment so this
installation will not interfere with any existing Python software.
Make sure to have at least python version 3.9 then::

    $ python3 -m venv /scratch/$USER/myvenv
    $ source /scratch/$USER/myvenv/bin/activate
    $ pip install --upgrade pip

Install the package in edit mode which will also install all its dependencies::

    $ cd dls-normsql
    $ pip install -e .[dev]

Now you may begin modifying the code.

|

If you plan to modify the docs, you will need to::

    $ pip install -e .[docs]

    


.. # dae_devops_fingerprint 06258b784a6611f9c5473dd7ab11d8c1
