package block

// BackupOutputFormat defines the format of the backup output.
type BackupOutputFormat string

// Constants for BackupFormat to specify the output format.
const (
	BackupOutputFormatSTDOUT BackupOutputFormat = "stdout"
	BackupOutputFormatFile   BackupOutputFormat = "file"
)

// BackupConfig is the configuration for a backup operation.
type BackupConfig struct {
	// Store is the sqlite data store used to persist the backup metadata.
	Store *Store
	// DevicePath is the path to the device/file to backup.
	DevicePath string
	// Output format for the backup.
	OutputFormat BackupOutputFormat
	// OutputDirectory is the directory where the backup will be written.
	// If OutputFormat is set to STDOUT, this field is ignored.
	OutputDirectory string
	// OutputFileName is the name of the backup file.
	// If OutputFormat is set to STDOUT, this field is ignored.
	OutputFileName string
	// BlockSize is the number of bytes used to calculate the hash.
	// WARNING: Changing this value will invalidate all previous backups.
	BlockSize int
	// BlockBufferSize is the number of blocks to buffer before hashing and writing to storage.
	// This is used to reduce the number of writes to storage and improve performance.
	BlockBufferSize int
}

// RestoreInputFormat defines the format of the incoming backup.
type RestoreInputFormat string

// Constants for RestoreFormat to specify the input format.
const (
	RestoreInputFormatFile RestoreInputFormat = "file"
)

const (
	backupTypeFile = "file"
)

// RestoreConfig is the configuration for a restore operation.
type RestoreConfig struct {
	// Store is the sqlite data store used to persist the backup metadata.
	Store *Store
	// RestoreInputFormat is the format of source data.
	RestoreInputFormat RestoreInputFormat
	// SourceBackupID is the ID of the backup to restore.
	SourceBackupID int
	// OutputDirectory is the directory where the backup will be restored.
	OutputDirectory string
	// OutputFileName is the name of the restored file.
	OutputFileName string
}
