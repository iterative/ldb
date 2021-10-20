# ldb init

## Quickstart defaults
The simplest way to create a new repo is without any arguments:
```
ldb init
```

This is the init method used to automatically jumpstart a new instance if another command such as `ldb add` is used and there's not already an instance.

By default, this will create a new LDB instance at `~/.ldb/personal_instance`, but this location can be changed without passing an argument in a couple of ways.

The first is to set the `LDB_DIR` environment variable:
```
LDB_DIR=some/directory/path/ ldb init
```

The second is to set the `core.ldb_dir` configuration value to an absolute path in the global configuration TOML file, `~/.ldb/config`:
```
[core]
ldb_dir = '/some/absolute/path
```

## Additional Options

If the LDB dir specified already contains an existing LDB instance, then the command fails. But if the `-f` or `--force` option is used, then the existing instance will be erased and a new instance initialized.

If the LDB dir specified contains other data, then the command fails. The user must provide an empty directory.

The `ldb init` command creates the following directory structure in the LDB dir specified. LDB considers any directory containing this structure to an LDB instance regardless of the presence of other files and directories.
```
.
├── data_object_info/
├── datasets/
└── objects/
    ├── annotations/
    ├── collections/
    └── dataset_versions/
```

The user can also pass the location of a directory as an argument. This will override the `LDB_DIR` environment variable and the `core.ldb_dir` configuration.
```
ldb init some/path
```


# ldb add-storage

# ldb add

# ldb del

# ldb tag

# ldb instantiate

# ldb commit

# ldb stage

# ldb index

# ldb diff

# ldb list

# ldb status

