# Data Quality Investigation Report
Generated: 2025-06-10

## Executive Summary

This report documents the investigation of critical data quality issues in the polling station geocoding pipeline.

### Key Findings:

- **Municipality Count Discrepancy**: Found 8085 municipalities (expected ~5,570)
- **Extreme Changes**: 140 municipalities with 100%+ change from 2022-2024
- **Panel Coverage Gap**: 89.8% coverage (target: 90%)

## Detailed Analysis

### 1. Municipality Count Analysis

```
      ano n_municipios
    <int>        <int>
 1:  2006         5565
 2:  2008         5565
 3:  2010         5567
 4:  2012         5570
 5:  2014         5570
 6:  2016         5568
 7:  2018         5570
 8:  2020         5568
 9:  2022         5570
10:  2024         5569
```

### 2. Temporal Consistency

Overlap matrix showing municipality counts shared between years:
```
     2006 2008 2010 2012 2014 2016 2018 2020 2022 2024
2006 5565 5565 5565 5565 5565 5563 5565 5563 5565 5563
2008 5565 5565 5565 5565 5565 5563 5565 5563 5565 5563
2010 5565 5565 5567 5567 5567 5565 5567 5565 5567 5565
2012 5565 5565 5567 5570 5570 5568 5570 5568 5570 5568
2014 5565 5565 5567 5570 5570 5568 5570 5568 5570 5568
2016 5563 5563 5565 5568 5568 5568 5568 5568 5568 5568
2018 5565 5565 5567 5570 5570 5568 5570 5568 5570 5568
2020 5563 5563 5565 5568 5568 5568 5568 5568 5568 5568
2022 5565 5565 5567 5570 5570 5568 5570 5568 5570 5568
2024 5563 5563 5565 5568 5568 5568 5568 5568 5568 5569
```
