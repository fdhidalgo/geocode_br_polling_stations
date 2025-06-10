# Data Quality Fix Report: 2024 Municipality Codes
Generated: 2025-06-10

## Summary

- Total records processed: 599,204
- Records successfully converted: 599,192 (100.0%)
- Valid IBGE codes after fix: 599,192
- Invalid/unmatched codes: 12

## Issue Identified

The 2024 polling station data uses TSE (Electoral Court) municipality codes instead of standard IBGE codes.
This caused:
- Municipality codes outside expected ranges (e.g., MT codes not in 51xxxxx range)
- Inability to match with other datasets using IBGE codes

## Fix Applied

Converted TSE codes to IBGE codes using official municipality identifier mapping.

## State-by-State Results

```
     SG_UF  total converted valid_ibge conversion_rate
    <char>  <int>     <int>      <int>           <num>
 1:     RO   6219      6219       6219       100.00000
 2:     AC   2380      2380       2380       100.00000
 3:     AM  12376     12376      12376       100.00000
 4:     RR   1511      1511       1511       100.00000
 5:     PA  25308     25308      25308       100.00000
 6:     AP   1929      1929       1929       100.00000
 7:     MA  20217     20217      20217       100.00000
 8:     PI  11099     11099      11099       100.00000
 9:     CE  31685     31685      31685       100.00000
10:     RN   9589      9589       9589       100.00000
11:     PB  13240     13240      13240       100.00000
12:     PE  23353     23353      23353       100.00000
13:     AL   7082      7082       7082       100.00000
14:     BA  38023     38023      38023       100.00000
15:     SE   7334      7334       7334       100.00000
16:     MG  59116     59116      59116       100.00000
17:     MS   9489      9489       9489       100.00000
18:     ES  11028     11028      11028       100.00000
19:     RJ  39832     39832      39832       100.00000
20:     SP 148063    148063     148063       100.00000
21:     TO   4965      4965       4965       100.00000
22:     PR  33230     33230      33230       100.00000
23:     SC  17188     17188      17188       100.00000
24:     RS  33936     33936      33936       100.00000
25:     GO  20859     20859      20859       100.00000
26:     MT  10153     10141      10141        99.88181
     SG_UF  total converted valid_ibge conversion_rate
```

## Recommendations

1. Update data import pipeline to handle TSE codes in future years
2. Add validation check for municipality code format
3. Document this code system change in data dictionary
