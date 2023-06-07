.. # ********** Please don't edit this file!
.. # ********** It has been generated automatically by dae_devops version 0.5.3.
.. # ********** For repository_name dls-normsql

Documenting
=======================================================================

If you plan to make update the documentation in this repository, you can use the steps below.

First, follow the steps in the Developing section to get a copy of the source code and install its dependencies.

If you didn't do this already, make sure you have the documentation tools::

    $ cd <your development area>/dls-normsql
    $ pip install -e .[docs]

To produce the documentation locally::

    $ tox -q -e docs

This writes the html into local directory build/html.  You can browse the local documentation by::

    file:///<your development area>/dls-normsql/build/html/index.html

When you push either the main branch or a tag to GitHub, the documents are built and published automatically to this url::

    https://diamondlightsource.github.io/dls-normsql/main/index.html


.. # dae_devops_fingerprint 2e96ce2e1701d5674aa92004b95cb6dd
