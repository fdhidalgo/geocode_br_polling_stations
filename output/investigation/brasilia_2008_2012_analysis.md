# Investigation: Brasília in 2008/2012 Municipal Election Data

## Summary
Brasília (Distrito Federal) appears in the polling station data for 2008 and 2012, which are municipal election years. This is unexpected because Brasília, as a Federal District, does not hold municipal elections (no mayor or city council).

## Findings

### 1. Data Presence
- Brasília has 523 polling stations in 2008 data
- Brasília has 579 polling stations in 2012 data
- All entries use TSE code 97012 for Brasília
- The polling stations are real locations (schools, community centers)

### 2. Election Calendar Context
Brazilian elections follow a 2-year cycle:
- **Federal/State elections**: 2006, 2010, 2014, 2018, 2022
  - President, Governor, Senators, Federal/State Deputies
- **Municipal elections**: 2008, 2012, 2016, 2020, 2024
  - Mayor, City Councilors

### 3. Root Cause Analysis
The issue appears to stem from the TSE source data structure:

1. **TSE Data Collection**: The TSE maintains a comprehensive database of ALL polling stations, regardless of which elections they're used for.

2. **No Election Type Filter**: The raw data file (`polling_stations_2006_2024.csv.gz`) does not contain a field indicating which type of election the polling station was used for.

3. **Infrastructure vs Usage**: The data represents polling station infrastructure that exists year-round, not necessarily their usage in specific elections.

## Explanation
The presence of Brasília polling stations in 2008/2012 data is likely because:

1. **Physical Infrastructure**: The polling stations physically exist and are maintained by electoral authorities
2. **Data Collection Practice**: TSE updates their polling station registry periodically, including during municipal election years
3. **Administrative Updates**: Changes to polling locations, addresses, or capacities may be recorded during any election cycle

## Recommendations

### Option 1: Document the Behavior
Add documentation explaining that the dataset includes all polling station infrastructure updates, regardless of whether the location was used for voting in that specific year.

### Option 2: Add Election Type Filtering
Create a filtering mechanism based on known patterns:
- Brasília (DF) and Fernando de Noronha (PE) should only have data for federal/state election years
- Regular municipalities should have data for all election years

### Option 3: Add Metadata
Include a flag or field indicating whether a polling station was actually used for voting in that year, or just updated in the infrastructure database.

## Code Example for Filtering
```r
# Remove non-voting entries for special districts
filter_election_appropriate <- function(data) {
  # Define special districts that don't have municipal elections
  special_districts <- c(
    5300108,  # Brasília (DF)
    2605459   # Fernando de Noronha (PE)
  )
  
  # Municipal election years
  municipal_years <- c(2008, 2012, 2016, 2020, 2024)
  
  # Filter out special districts from municipal years
  data[!(cod_localidade_ibge %in% special_districts & ano %in% municipal_years)]
}
```

## Conclusion
This is not a data error but rather a characteristic of how TSE maintains its polling station database. The infrastructure exists and is tracked continuously, even if not used for voting in certain years. Any filtering should be done with clear documentation of the reasoning.