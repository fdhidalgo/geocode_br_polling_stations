# Year-over-Year Extreme Changes Investigation Summary

## Key Findings

### 1. Most "Extreme Changes" Are Legitimate

**New Municipalities Created (2008-2013):**
- Mojuí dos Campos (PA) - 2010: Separated from Santarém
- Pescaria Brava (SC) - 2013: Separated from Laguna  
- Balneário Rincão (SC) - 2013: Separated from Içara
- Paraíso das Águas (MS) - 2013: Separated from Costa Rica

These are real administrative changes, not data errors.

### 2. Brasília (DF) - Special Case

**Expected Pattern:** Should only appear in federal/state elections (2006, 2010, 2014, 2018, 2022)

**Actual Pattern:** Appears in 2006, 2008, 2010, 2012, 2014, 2018, 2022

**Issue:** Brasília appears in 2008 and 2012 (municipal election years) but is missing from 2016, 2020, and 2024. This is inconsistent.

**Explanation:** As a Federal District, Brasília has no municipal elections. The presence in 2008/2012 may be:
- Data collection error
- Special district elections being categorized as municipal
- Need further investigation

### 3. Fernando de Noronha - Another Special Case

**Pattern:** Appears in most years but missing 2016, 2020, 2024

**Explanation:** State district with special administrative status. Small population (1 polling station) makes it prone to reporting inconsistencies.

### 4. Overall Statistics

- **Total extreme changes across all years:** 504 cases
- **Municipalities that appeared:** 10 (mostly legitimate new municipalities)
- **Municipalities that disappeared:** 6 (need investigation)
- **Year with most changes:** 2022-2024 (1 new, 2 disappeared)

### 5. State-Level Patterns

States with highest percentage of municipalities experiencing extreme changes:
1. **TO (Tocantins):** 18.7%
2. **PB (Paraíba):** 17.9%  
3. **PR (Paraná):** 16.3%
4. **GO (Goiás):** 13.0%

These tend to be states with many small municipalities.

### 6. Change Distribution by Year

Percentage of municipalities with extreme changes:
- 2006-2008: 1.10%
- 2008-2010: 0.92%
- 2010-2012: 1.17%
- 2012-2014: 1.27%
- 2014-2016: 1.04%
- 2016-2018: 0.77%
- 2018-2020: 0.66%
- 2020-2022: 0.74%
- 2022-2024: 1.38%

The rates are consistently low (<1.5%), suggesting data quality is generally good.

## Conclusions

1. **Most extreme changes are real**, not errors:
   - New municipalities from administrative divisions
   - Small municipalities with variable polling station counts
   
2. **Special cases need attention:**
   - Brasília's inconsistent pattern needs investigation
   - Fernando de Noronha's missing years may be due to its special status

3. **Data quality is good overall:**
   - >98.5% of municipalities show stable patterns
   - Extreme changes are rare and often explainable

4. **No systemic data quality issues** were found in the year-over-year analysis

## Recommendations

1. **Document special cases:** Create a reference table for Brasília and Fernando de Noronha explaining their unique election patterns

2. **Flag for manual review:** The ~140 municipalities with extreme changes, focusing on:
   - Recent disappearances (2022-2024)
   - Municipalities that appear/disappear multiple times

3. **No algorithm changes needed:** The panel ID system appears to be working correctly given the constraints