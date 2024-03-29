==============
k2hash_python
==============

.. image:: https://img.shields.io/badge/license-MIT-blue.svg
        :target: https://github.com/yahoojapan/k2hash_python/blob/master/LICENSE
.. image:: https://img.shields.io/pypi/pyversions/k2hash.svg
        :target: https://pypi.python.org/pypi/k2hash
.. image:: https://img.shields.io/github/forks/yahoojapan/k2hash_python.svg
        :target: https://github.com/yahoojapan/k2hash_python/network
.. image:: https://img.shields.io/github/stars/yahoojapan/k2hash_python.svg
        :target: https://github.com/yahoojapan/k2hash_python/stargazers
.. image:: https://img.shields.io/github/issues/yahoojapan/k2hash_python.svg
        :target: https://github.com/yahoojapan/k2hash_python/issues
.. image:: https://github.com/yahoojapan/k2hash_python/workflows/Python%20package/badge.svg
        :target: https://github.com/yahoojapan/k2hash_python/actions
.. image:: https://readthedocs.org/projects/k2hash-python/badge/?version=latest
        :target: https://k2hash-python.readthedocs.io/en/latest/?badge=latest
.. image:: https://img.shields.io/pypi/v/k2hash
        :target: https://pypi.org/project/k2hash/
   
Overview
---------

k2hash_python is an official python driver for `k2hash`_.

.. _`k2hash`: https://k2hash.antpick.ax/

.. image:: https://raw.githubusercontent.com/yahoojapan/k2hash_python/main/docs/images/top_k2hash_python.png


Install
--------

Firstly you must install the k2hash shared library::

    curl -o- https://raw.github.com/yahoojapan/k2hash_python/master/utils/libk2hash.sh | bash

Then, let's install k2hash using pip::

    pip install k2hash


Usage
------

Try to set a key and get it::

    import k2hash
    
    k = k2hash.K2hash('test.k2h')
    k.set('hello', 'world')
    v = k.get('hello')
    print(v)    // world


Development
------------

Clone this repository and go into the directory, then run the following command::

    $ python3 -m pip install --upgrade build
    $ python3 -m build


Documents
----------

Here are documents including other components.

`Document top page`_

`About K2HASH`_

`About AntPickax`_

.. _`Document top page`: https://k2hash-python.readthedocs.io/
.. _`ドキュメントトップ`: https://k2hash-python.readthedocs.io/
.. _`About K2HASH`: https://k2hash.antpick.ax/
.. _`K2HASHについて`: https://k2hash.antpick.ax/
.. _`About AntPickax`: https://antpick.ax
.. _`AntPickaxについて`: https://antpick.ax


Packages
--------

Here are packages including other components.

`k2hash(python packages)`_

.. _`k2hash(python packages)`:  https://pypi.org/project/k2hash/


License
--------

MIT License. See the LICENSE file.

AntPickax
---------

**k2hash_python** is a project by AntPickax_, which is an open source team in `Yahoo Japan Corporation`_.

.. _AntPickax: https://antpick.ax/
.. _`Yahoo Japan Corporation`: https://about.yahoo.co.jp/info/en/company/

