import boto3
from datetime import datetime

rds_client = boto3.client('rds')
s3_client = boto3.client('s3')

# Variables
RDS_INSTANCE_ID = 'your-rds-instance-id'
S3_BUCKET = 'your-s3-bucket-name'
SNAPSHOT_PREFIX = 'rds-snapshot-'

def create_rds_snapshot():
    """Creates an RDS snapshot."""
    snapshot_id = SNAPSHOT_PREFIX + datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    
    try:
        print(f"Creating snapshot {snapshot_id} for RDS instance {RDS_INSTANCE_ID}")
        response = rds_client.create_db_snapshot(
            DBSnapshotIdentifier=snapshot_id,
            DBInstanceIdentifier=RDS_INSTANCE_ID
        )
        print(f"Snapshot {snapshot_id} creation started")
        return response['DBSnapshot']['DBSnapshotIdentifier']
    except Exception as e:
        print(f"Error creating snapshot: {str(e)}")
        return None

def upload_snapshot_metadata_to_s3(snapshot_id):
    """Uploads snapshot metadata to S3."""
    try:
        print(f"Uploading metadata for snapshot {snapshot_id} to S3")
        
        metadata = f"Snapshot ID: {snapshot_id}\nCreated at: {datetime.now()}\n"
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=f"{snapshot_id}.txt",
            Body=metadata
        )
        
        print(f"Snapshot metadata {snapshot_id}.txt uploaded to S3 bucket {S3_BUCKET}")
        return True
    except Exception as e:
        print(f"Error uploading snapshot metadata: {str(e)}")
        return False

def delete_old_snapshots(retention_days=7):
    """Deletes RDS snapshots older than retention_days."""
    try:
        snapshots = rds_client.describe_db_snapshots(DBInstanceIdentifier=RDS_INSTANCE_ID)['DBSnapshots']
        for snapshot in snapshots:
            snapshot_date = snapshot['SnapshotCreateTime'].replace(tzinfo=None)
            age_in_days = (datetime.now() - snapshot_date).days

            if age_in_days > retention_days:
                print(f"Deleting old snapshot: {snapshot['DBSnapshotIdentifier']}")
                rds_client.delete_db_snapshot(DBSnapshotIdentifier=snapshot['DBSnapshotIdentifier'])
                print(f"Deleted snapshot: {snapshot['DBSnapshotIdentifier']}")
    except Exception as e:
        print(f"Error deleting old snapshots: {str(e)}")

def main():
    snapshot_id = create_rds_snapshot()
    if snapshot_id:
        upload_snapshot_metadata_to_s3(snapshot_id)
        delete_old_snapshots(retention_days=7)

if __name__ == '__main__':
    main()
