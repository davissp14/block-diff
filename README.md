# block-diff

A block-level differential backup and restore prototype


# Usage

## Creating a backup
```bash
go run cmd/bdiff/main.go backup create ../test.rb --block-size 1
No output directory specified. Saving backup file to current directory.
Performing backup of ../test.rb to .
Backup completed successfully!
=============Info=================
Backup Duration: 51.429083ms
Backup file: ./test.rb_full_1716217003483
Backup size 78B
Source device size: 2.1 KiB
Space saved: 2.1 KiB
Blocks evaluated: 2191
Blocks written: 78
==================================
```

## Listing backups
```bash
go run cmd/bdiff/main.go backup list
|----|------|------------|--------------|---------|------------------------------|-------------------------------|
| ID | TYPE | BLOCK SIZE | TOTAL BLOCKS | SIZE    | CREATED AT                   |                               |
|----|------|------------|--------------|---------|------------------------------|-------------------------------|
| 1  | FULL | 1          | 2191         | 2.1 KiB | ./test.rb_full_1716217003483 | 2024-05-20 14:56:43 +0000 UTC |
|----|------|------------|--------------|---------|------------------------------|-------------------------------|

```


