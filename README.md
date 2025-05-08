# Core-Data-Utils

A lightweight Python framework for managing and transforming datasets with a focus on data independence and type flexibility.

## Key Features

- Type-agnostic dataset management
- Deep copy data protection
- Parallel processing support for transformations
- Easy dataset creation and manipulation

## Basic Usage

```python
from core_data_utils.datasets import BaseDataSet
from core_data_utils.transformations import BaseDataSetTransformation

# Create a dataset from a dictionary
data = {1: "hello", 2: "world", 3: "!"}
dataset = BaseDataSet.from_flat_dicts(data, metadata={"name": "greeting"})

# Data entries are protected from external modifications
data[1] = "goodbye"  # Won't affect the dataset

# Create custom transformations
class MyTransformation(BaseDataSetTransformation):
    def _transform_single_entry(self, entry, dataset_properties):
        return BaseDataSetEntry(
            entry.identifier,
            data=entry.data.upper(),
            metadata=entry.metadata
        )

# Apply transformations (supports parallel processing)
transformer = MyTransformation()
transformed_dataset = transformer(dataset, cpus=4)
```

## Use Cases

- Data preprocessing pipelines
- Dataset versioning and tracking
- Parallel data transformations
- When you need a consistent interface for handling different types of data
- Scenarios requiring data isolation and immutability

## Example: Number Processing

```python
# Creating a dataset of numbers
numbers = {i: i * 2 for i in range(5)}
dataset = BaseDataSet.from_flat_dicts(numbers)

class SquareNumTransformation(BaseDataSetTransformation):
    def _transform_single_entry(self, entry, dataset_properties):
        return BaseDataSetEntry(
            entry.identifier,
            data=entry.data ** 2,
            metadata=entry.metadata
        )

# Squaring all numbers using a transformation
square_transformer = SquareNumTransformation()
squared_dataset = square_transformer(dataset)

# Save/Load datasets
dataset.to_pickle("my_dataset.pkl")
loaded_dataset = BaseDataSet.from_pickle("my_dataset.pkl")
```

## Installation

```bash
pip install git+https://github.com/lettlini/core-data-utils
```
