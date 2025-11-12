# Best Practices for PySpark Aggregations
- Filter early: reduce data suze before performing aggregations
- Handle data types: Ensure data is clean and correctly typed 
- Avoid operations that use the entire dataset: Minimize operations like groupBy()
- Choose the right interface: prefer DataFrames for most tasks due to theiroptimizations
- Monitor performance: use `explain()` to unspect the execution plan and optimize accordingly