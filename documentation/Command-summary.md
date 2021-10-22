
## LDB datasets

Datasets are collections of pointers to data objects in storage locations, paired with optional annotations and metadata. Since datasets are collections, LDB can modify them without moving the actual data. To modify dataset membership, LDB stages it in a workspace (see below). To examine the physical data files or annotations in dataset, it must be partially or fully instantiated (data files copied over from storage to workspace). 

LDB datasets support names with [a-Z0-9-_] ANSI characters. LDB commands require dataset names to have a mandatory `ds:` prefix, and may take an optional `.v[0-9]*` postfix that denotes a version number.  
 
## LDB workspace
Workspace holds an internal structure for a dataset that is being modified. If LDB repository has no dataset `<name>`, a new dataset is created. If `<name>` references an existing dataset, it is staged out (but not instantiated). One user might have several workspaces in different directories.

Here is the structure of a workspace folder:
```
.ldb_workspace/
            ├── collection/
            └── workspace_dataset    
```
Any changes to a dataset (adding & removing objects, adding tags, etc) remain local to workspace until the `COMMIT` command. 

Most LDB commands – `ADD`, `DEL`, `LIST`, `STATUS`, `TAG`, `INSTANTIATE`, `COMMIT`, `STATUS` target a staged dataset, and hence must run from a workspace.

## locating an LDB instance

Every LDB command is linked to an instance where datasets and annotations are stored. There are two ways to locate an instance:

1. Set `core.ldb_dir` in the global configuration file `~/.ldb/config` to an absolute path.
```
[core]
ldb_dir = '/some/absolute/path'
```
2. Set the `LDB_DIR` environment variable to any absolute or relative path.

If both ways of configuration are present, the environment variable takes precedence.
If no method of configuration succeeds, all LDB commands will fail, except for `INIT` which does not require an existing installation, and `STAGE` when used in QuickStart (see below).

## Quickstart 

QuickStart allows the individual user to start using LDB immediately by means of creating datasets from cloud and local locations without any additional configuration. QuickStart is designed to lower barrier of trying LDB for individual users who need to get value from LDB in 1-2 commands.

`STAGE` is the only LDB command that trigger QuickStart. To do it, `STAGE` confirms the absence of an active LDB repository, and runs `ldb init ~/.ldb/private_instance` to create a new LDB instance before proceeding with staging a dataset.

 Under the hood, QuickStart process consists of the following two steps:

* LDB instance is created in the user's home directory.
* Default storage configuration is created with relaxed settings: 
    * All cloud locations are permitted to host data objects.
    * A `read-add` folder is created in user's home directory (see `ADD-STORAGE`).
 
Below is an example of QuickStart, where user queries a remote storage in two commands after LDB installation:

```
$ ldb stage <new-dataset> 
$ ldb add gs://iterative/roman-numerals --query class == "i"
```


# INIT \<directory\>

`INIT` creates a new LDB instance (repository) in the given directory. For most enterprise installations, LDB repository folder must be a shared directory on a fast disk. `INIT` does not attempt to locate an active LDB instance, and permits to create a new LDB repository anywhere in a filesystem.

In addition to creating an LDB instance, `INIT` makes a global configuration file at `~/.ldb/config` and sets the `core.ldb_dir` key to point to a new LDB location. If configuration files already exist, `INIT` does not change them.

Running `ldb init <path>` creates the following directory structure:
```
path/
├── data_object_info/
├── datasets/
└── objects/
    ├── annotations/
    ├── collections/
    └── dataset_versions/
```
After finishing, `INIT` prints the summary of work and a reminder on how to change active LDB instance pointers (see locating LDB instance).

## flags

`-f` or  `--force` 

If a target directory already contains an existing LDB instance,  `INIT` fails & prints a reminder to use `--force`.  Using `-f` or  `--force` erases an existing LDB installation.

If the target directory contains data, but not an LDB instance, `INIT` fails without an option to override. The user must provide an empty directory.


# ADD-STORAGE \<storage-URI\>

`ADD-STORAGE` registers a disk (or cloud) data storage location into LDB and verifies the requisite permissions. 

LDB keeps track of storage locations for several reasons, the primary being engineering discipline (prevent adding objects from random places), and authentication (see `access configuration` below). 

LDB supports the following storage URI types:  fs, Google Cloud, AWS, and Azure. 

The minimum and sufficient set of permissions for LDB is to **list, stat and read** any objects at `<storage-URI>`. `ADD-STORAGE` fails if permissions are not sufficient, and succeeds with a warning if permissions are too wide. `ADD-STORAGE` also checks if `<storage-URI>` falls within an already registered URI, and prints an error if this the case. Permissions are re-checked if existing storage location is added again.
  
One notable exception to "read-only permissions" can be the URI marked `read-add`, which is required if user wants to add local-fs data objects outside of the registered storage locations (for example, from within personal workspace). 

## flags

`--read-add` 

Storage location registered with this flag must allow for adding files. 

LDB supports at most one read-add location, and uses it to store _previously unseen_ local data files that `ADD` command may reference outside the registered storage. Users can change or remove the `read-add` attribute by repeatedly adding locations with or without this flag. Attempt to add a second`read-add` location to LDB should fail prompting the user to remove the attribute from existing location first. 

`read-add` location should never be used to store any data objects that originate at cloud locations. 

*Use scenario 1.* 

There is one storage location `gs://storage` registered (no flags). User tries to add file `cat1.jpg` from his workspace to a dataset. If LDB does not have an object with identical hashsum already indexed in storage, the ADD command fails:

```
$ ldb add ./cat.jpg
     error: object 0x564d is not in LDB and no read-add location configured
$
```

    
*Use scenario 2.* 

There is one storage location `gs://storage` registered (no flags), and another location `gs://add-storage` registered with `read-add`.  User tries to add file `cat1.jpg` from a workspace into a dataset. If LDB does not have an object with identical hashsum already indexed, the `ADD` command will copy `cat1.jpg` into `gs://add-storage` under unique folder name, index it, and add this object to a dataset.

```
$ ldb add ./cat.jpg
     warning: object 0x564d copied to gs://add-storage/auto-import220211-11
$
```

## access configuration

TODO document storage access configuration here

TODO object lambda access configuration here


# STAGE \<ds:\<name\>[.v\<number\>]\>  \<workspace_folder\>

`STAGE` command creates an LDB workspace at a given `<workspace_folder>` for dataset `<name>`. The destination folder is expected to be empty. 

If workspace is not empty, `STAGE` checks if it holds a clean dataset, and clobbers it silently.  If `<workspace_folder>` holds a dirty dataset, a warning and a status of this dataset are printed before failure and a reminder to use `--force` to override. If `<workspace_folder>` is not empty but does not hold an LDB dataset, just a reminder for `--force` is printed. 

If `STAGE` cannot locate an active LDB instance, it assumes a QuickStart, and proceeds with setting a new LDB instance (see QuickStart discussion).
    
## flags

`-f` or  `--force` 

allow to clobber the workspace regardless of what is there.
    

# INDEX \<storage folder URI(s) | storage file(s) URI(s)\> [flags]

`INDEX` command updates the LDB repository with data objects and annotations given as arguments. If folder is given and no format flag provided, this folder is traversed recursively to recover objects and annotations in a default format. 

`INDEX` fails if cannot find annotations conformant with expected format. `INDEX` assumes argument URIs to reside within storage locations configured (see `ADD-STORAGE`) and will fail otherwise.

## flags

`--format < COCO | OpenImage | ImageNet | folder-labels >`  

Set the expected locations of data objects and annotations according to format specified. 

# ADD  \<0x\<sum\> | object_path | ds:\<name\>[.v\<num\>] \> [filters]

`ADD` is the main command of the application. It adds data sample(s) to a dataset. `ADD` takes object identifiers in various forms as the main argument, and applies optional filters specified as flags. Objects passing the filter are merged with a staged dataset. 

`ADD` allows for multiple objects (or object groups) of one type to be specified in one command, and applies filters to all objects referenced in the order they are processed. 

## arguments types supported by `ADD`

`object_path` - any valid path registered with `ADD-STORAGE` to objects and annotations in storage that work as objects containers. The path can be fully qualified (down to an object), or reference a folder. In all cases, `ADD` calls `INDEX` to index the content of path first.

`object_path` - any valid path NOT registered with `ADD-STORAGE` to objects and annotations on the *local fs* that work as objects containers. The path can be fully qualified (down to objects), or reference a folder. When `ADD` is called on unregistered local fs path, it works in the following way:

* If updated annotations for objects in a dataset are present, they are added to LDB and revisions are bumped in a dataset. 
* If objects that are already indexed in LDB but not assigned to this dataset are present, they are added to dataset.
* If new objects (missing in LDB) are present, they are added to `read-add` storage, indexed there, and then added to dataset.
* If new objects (missing in LDB) are present, but no `read-add` storage configured, command fails.

Use case 1: 
```
$ ldb stage ds:cats
$ cp ~/storage/cat1.jpg ./   # this object is already in LDB but not in this dataset
$ ldb add ./                 # result: staged dataset ds:cats now includes cat1.jpg
```

Use case 2: 
```
$ ldb stage ds:cats
$ cp ~/Downloads/cat1.jpg ./   # this object is not in LDB and read-add storage is configured
$ ldb add ./                   # result: cat1.jpg copied to read-add storage, and added
```

Use case 3: 
```
$ ldb stage ds:cats
$ ldb instantiate
$ sed -i 's/class=cat/class=dog/g' cat1.json 
$ ldb add ./                   # result: annotation for cat1.jpg got a new version in LDB, and ds:cats has it too
```

`0xFFFFFF` - full hashsum of an object. Currently LDB uses MD5, so any MD5 hashsum that corresponds to an object indexed by LDB is a valid identifier.

Use case 1: 
```
$ ldb stage ds:cats
$ ldb add 0x456FED 0x5656FED    # result: objects id 0x456FED 0x5656FED added to dataset
```

ds:\<name\>[.v\<num\>] - dataset name with an optional version number. Any valid LDB dataset can serve as a source of objects.

Use case 1: 
```
$ ldb stage ds:cats
$ ldb add ds:black_cats ds:white_cats.v2  # result: merged with latest ds:black_cats and ver. 2 of ds:white_cats
```


## filters supported by `ADD`

`ADD` can be called with several filter flags. If multiple flags are specified, filters are pipelined, so the order may matter. Multiple filters of one type are not permitted in one command.

--file \<filesystem attributes query\>

--query \<annotation query terms\>

--ml \<model with arguments\>

--limit \<integer\>

--sample \<integer\>

--version \<interger\>

--user_version \<number\>

--tag \<string\>

## Query Language







# ldb del

# ldb tag

# ldb instantiate

# ldb commit

# ldb index

# ldb diff

# ldb list

# ldb status

