# locating an LDB instance

Every LDB command is linked to an instance, where datasets and annotations are stored. There are two ways to locate an instance:

1. configuration file `~/.ldb/config`
```
[core]
ldb_dir = '/some/absolute/path'
```
3. `LDB_DIR` environment variable.

If both ways of configuration are present, environment variable takes the precedence.
If no method of configuration succeeds, all LDB commands will fail, a sole exception being `STAGE` command when used in QuickStart (see below).

# INIT [directory]

`INIT` creates a new LDB instance (repository) in a given directory. 

For most enterprise installations, this folder must be a shared directory on a fast disk.
In addition to creating an instance, INIT makes a configuration file at `~/.ldb/config` and sets `ldb_dir` key in it to a new LDB location (this step is skipped if configuration already exists). 

The `ldb init` command creates the following directory structure in the LDB dir specified:
```
.
├── data_object_info/
├── datasets/
└── objects/
    ├── annotations/
    ├── collections/
    └── dataset_versions/
```
LDB considers any directory containing this structure a valid LDB instance regardless of the presence of other files and directories.

If `[directory]` argument is omitted, LDB defaults to creating a *private* repository at `~/.ldb/private_instance`. 

Unlike *enterprise* installation, *private* instance has a default `read-add` storage location configured at `~/.ldb/private_instance/add-storage` (see `ADD-STORAGE` below) where the locally added data objects will be copied and stored. The differences between private and enterprise LDB instances are mostly limited to how LDB treats previously unseen data objects (see `ADD` below).

## flags

`-f` or  `--force` 

If a target directory already contains an existing LDB instance,  `INIT` fails & prints a reminder to use `--force`.  Using `-f` or  `--force` erases an existing LDB installation.

If the target directory contains any data other than LDB instance, `INIT` fails without an option to override. The user must provide an empty directory.


# ADD-STORAGE \<storage-URI\>

`ADD-STORAGE` registers a disk (or cloud) storage location into LDB and verifies the requisite permisssions. 

LDB keeps track of storage locations for several reasons, the primary being engineering discipline (prevent adding objects from random places). LDB supports the following storage URI types: fs, Google Cloud, AWS, and Azure.

The minimum and sufficient set of permissions for LDB is to **list, stat and read** any objects at `<storage-URI>`. `ADD-STORAGE` fails if permissions are not sufficient, and succeeds with a warning if permissions are too wide. `ADD-STORAGE` also checks if `<storage-URI>` falls within an already registered URI, and prints an error if this the case. Permissions are re-checked if the same storage location is added again.
  
One exception to "read-only permissions rule" can be the URI marked `read-add`, which is required if user wants to add local data objects outside of the registered storage locations (for example, from within personal workspace). This scenario is optional for *enterprise* LDB, and supported by default for *private* instances.

## flags

`--read-add` 

Storage location registered with this flag must allow for adding files. 

LDB supports at most one read-add location, and uses it to store _previously unseen_ local data files that `ADD` command may reference outside the registered storage. Users can change or remove the `read-add` attribute by repeatedly adding locations with or without this flag. Attempt to add a 2nd `read-add` location to LDB should fail prompting the user to remove the attribute from existing location first. 

`read-add` location should never be used to store any data objects that originate at cloud locations. If an *enterprise* user tries to `ADD` a new object from cloud URI not registered with `ADD-STORAGE`, the `ADD` command should fail. If a *private-instance* user tries to `ADD` a new object from cloud URI not registered with `ADD-STORAGE`, the command should succeed.

*Use scenario 1.* 

There is one storage location `gs://storage` registered (no flags). User tries to add file `cat1.jpg` from his workspace to a dataset. If LDB does not have an object with identical hashsum already indexed in storage, the ADD command fails:

```
$ ldb add ./cat.jpg
     error: object 0x564d is not in LDB and no read-add location configured
$
```

Note that this scenario does not exist for *private* instances, which have `read-add` location configured by default.
    
*Use scenario 2.* 

There is one storage location `gs://storage` registered (no flags), and another location `gs://add-storage` registered with `read-add`.  User tries to add file `cat1.jpg` from a workspace into a dataset. If LDB does not have an object with identical hashsum already indexed, the `ADD` command will copy `cat1.jpg` into `gs://add-storage` under unique folder name, index it, and add this object to a dataset.

```
$ ldb add ./cat.jpg
     warning: object 0x564d copied to gs://add-storage/auto-import220211-11
$
```

## access configutation

TODO document storage access configuration here


# STAGE \<ds:\<name\>[.v\<version number\>]\>  \<workspace_folder\>

`STAGE` command creates an LDB workspace at a given `<workspace_folder>` for dataset `<name>`. The destination directory is expected to be empty, and `STAGE` fails otherwise. If `<workspace_folder>` already has another LDB dataset, a warning and status of this dataset are printed, along with a reminder to use `--force`. If `<workspace_folder>` is not empty but does not hold an LDB dataset, just a reminder to use `--force` is printed. 
    
```
TODO outline workspace structure    
    
```
    
LDB workspace holds an internal structure for a dataset that is being modified. If LDB repository has no dataset `<name>`, a new dataset is created. If `<name>` references an existing dataset, it is staged out (but not instantiated). One user might have several workspaces in different directories.

Any changes to a dataset (adding & removing objects, adding tags, etc) remain local to workspace until the `COMMIT` command. Most LDB commands – `ADD`, `DEL`, `LIST`, `STATUS`, `TAG`, `INSTANTIATE`, `COMMIT` either require to run from within a workspace, or operate on a staged dataset when launched from a workspace.

## flags

`-f` or  `--force` 

allow to clobber the workspace.
    
## Dataset naming conventions

LDB datasets support names with [a-Z0-9-_] ANSI characters (TODO: worry about UTF charset?). LDB commands require dataset names to have a mandatory `ds:` prefix, and an optional `.v[0-9]*` postfix that denotes a version number.  
    
## Quickstart behavior

`STAGE` is the only LDB command that can be run without an LDB instance already configured. 
 As part of the first-time user QuickStart, `STAGE` detects the absence of LDB repositories on a system, and runs `ldb init` to create a new private instance before proceeding to staging.


# ldb add

# ldb del

# ldb tag

# ldb instantiate

# ldb commit

# ldb index

# ldb diff

# ldb list

# ldb status

