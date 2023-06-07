.. # ********** Please don't edit this file!
.. # ********** It has been generated automatically by dae_devops version 0.5.3.
.. # ********** For repository_name dls-normsql

Installing
=======================================================================


You will need python 3.10 or later. 

On a Diamond Light Source internal computer, you can achieve Python 3.10 by::

    $ module load python/3.10

You can check your version of python by typing into a terminal::

    $ python3 --version

It is recommended that you install into a virtual environment so this
installation will not interfere with any existing Python software::

    $ python3 -m venv /scratch/$USER/myvenv
    $ source /scratch/$USER/myvenv/bin/activate
    $ pip install --upgrade pip


You can now use ``pip`` to install the package and its dependencies::

    $ python3 -m pip install dls-normsql

If you require a feature that is not currently released, you can also install
from git::

    $ python3 -m pip install git+https://gitlab.diamond.ac.uk/kbp43231/dls-normsql.git

The package should now be installed and the command line should be available.
You can check the version that has been installed by typing::

    $ dls-normsql --version
    $ dls-normsql --version-json

.. # dae_devops_fingerprint d972d49fe32bcad19880f2817fed8fae
