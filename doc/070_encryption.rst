..
  Normally, there are no heading levels assigned to certain characters as the structure is
  determined from the succession of headings. However, this convention is used in Python’s
  Style Guide for documenting which you may follow:

  # with overline, for parts
  * for chapters
  = for sections
  - for subsections
  ^ for subsubsections
  " for paragraphs

##########
Encryption
##########


*"The design might not be perfect, but it’s good. Encryption is a first-class feature,
the implementation looks sane and I guess the deduplication trade-off is worth
it. So… I’m going to use restic for my personal backups.*" `Filippo Valsorda`_

.. _Filippo Valsorda: https://words.filippo.io/restic-cryptography/

************************
Managing repository keys
************************

The ``key`` command allows you to set multiple access keys or passwords
per repository. In fact, you can use the ``list``, ``add``, ``remove``, and
``passwd`` (changes a password) sub-commands to manage these keys very precisely:

.. code-block:: console

    $ restic -r /srv/restic-repo key list
    enter password for repository:
     ID          User        Host        Created
    ----------------------------------------------------------------------
    *eb78040b    username    kasimir   2015-08-12 13:29:57

    $ restic -r /srv/restic-repo key add
    enter password for repository:
    enter password for new key:
    enter password again:
    saved new key as <Key of username@kasimir, created on 2015-08-12 13:35:05.316831933 +0200 CEST>

    $ restic -r /srv/restic-repo key list
    enter password for repository:
     ID          User        Host        Created
    ----------------------------------------------------------------------
     5c657874    username    kasimir   2015-08-12 13:35:05
    *eb78040b    username    kasimir   2015-08-12 13:29:57

Note that the currently used key is indicated by an asterisk (``*``).

Rotating keys from a roster
---------------------------

When keys have to be managed for several people, removing them one by one
with ``key remove`` is error prone: it is easy to lose track of which key
belongs to whom, and removing the wrong key can lock everyone out of the
repository. The ``key rotate`` sub-command reconciles the repository keys
with a roster of current member key IDs instead:

.. code-block:: console

    $ restic -r /srv/restic-repo key rotate --roster-file members.txt

The roster file contains one key ID (or unique ID prefix as shown by
``key list``) per line. Empty lines and lines starting with ``#`` are
ignored. The rules are:

* Every key listed in the roster is retained.
* Keys a previous rotation knew about that are missing from the roster are
  removed.
* Keys added manually with ``key add`` that never appeared in a roster are
  never removed, even if they are not listed.
* The key used to run the command must be part of the roster, so the
  repository is guaranteed to remain unlockable with a known key.

The first rotation records the provided roster as a *baseline* and does not
remove any keys; only later rotations can remove keys. The rotation is
applied atomically, an invalid roster or a write failure rolls back all
changes, and running the command again with the same roster does not modify
the repository. Use ``--dry-run`` to preview the changes:

.. code-block:: console

    $ restic -r /srv/restic-repo key rotate --roster-file members.txt --dry-run

A rotation state file is stored alongside the keys in the repository, so the
information which keys are rotation-managed travels with the repository.
This file is ignored by the other ``key`` commands.
