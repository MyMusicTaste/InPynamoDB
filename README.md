# PynamoDB_async
This transforms [PynamoDB](https://github.com/pynamodb/PynamoDB)'s basic methods working asynchronously.

## Requirements
- Python 3.5 and above

## Developed
### Models
- Model.create_table()
- Model.save()
- Model.query()

## TODO
### Models
- Model.delete_table()
- Model.delete()

### Indexes
- class GlobalSecondaryIndex
- class LocalSecondaryIndex
- Model.view_index.query()

### Batch Operations
- Model.batch_write()
- Model.batch_get()
- Model.scan()