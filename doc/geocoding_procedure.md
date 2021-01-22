Geocoding Brazilian Polling Stations with Administrative Data Sets
================

This document outlines an approach to geocoding Brazilian polling
station that heavily relies on administrative datasets. In addition to
detailing our approach, we also provide some evidence on the error of
our method and how it compares to the [Google Maps Geocoding
API](https://developers.google.com/maps/documentation/geocoding/overview).

Our general approach is to generate a series of potential coordinates
from a variety of administrative datasets. We use a machine learning
model trained on a subset of the data with coordinates provided by
Supreme Electoral Tribunal (*TSE*) to choose among the candidate
coordinates. Inputs to this model are mostly measures of the quality of
string matches between the polling station address and administrative
data sources, as well as other characteristics of the address and
municipality of the polling station. For each polling station, we select
the coordinates with the predicted smallest error among the possible
coordinates.

## Data Sources

To geocode the polling stations, we leverage three main data sources:

-   *Cadastro Nacional de Endereços para Fins Estatísticos* (CNEFE) from
    the 2010 Census.
-   *Cadastro Nacional de Endereços para Fins Estatísticos* from the
    2017 Agricultural Census.
-   *Catálogo de Escolas* from INEP.

The CNEFE datasets are national databases of addresses prepared by IBGE
for the census and include detailed data on streets and addresses. The
2010 version includes private addresses, as well as listings of
government buildings (such as schools) and the names of local
establishments (such as the names of schools or businesses). The 2017[1]
version only includes agricultural properties. Addresses in *rural*
census tracts (*setores censitários*) in the 2010 CNEFE have longitude
and latitude, while all agricultural properties in the 2017 CNEFE are
geocoded.

The 2010 Census data did not include coordinates for addresses in urban
census tracts. To partially overcome this issue, we compute the centroid
of the census tract and assign this coordinate to each property in the
urban census tract. Because urban census tracts tend to be compact,
tract centroid should still be fairly close to the true coordinates.
Nevertheless, this imputation step will lead to more error for urban
addresses than rural addresses.

The INEP data is a catalog of private and public schools with addresses
and longitude and latitude.[2]

## String Matching

To geocode polling stations, we use fuzzy string matching to match
polling stations to coordinates in the administrative datasets by name,
address, street, or neighborhood. This string matching procedure
generates several candidate coordinates. To choose among these possible
coordinates, we use a Random Forest model trained on a sample of polling
stations with coordinates provided by the election authorities.

The general approach is a follows:

1.  Normalize[3] name and address of polling station.

2.  Normalize addresses and school names in administrative datasets.

3.  Find the “medioid” (i.e. the median point) for all unique streets
    and neighborhoods in the CNEFE datasets.

4.  Compute the normalized Levenshtein string distance between polling
    station name and the names of schools in the INEP and 2010 CNEFE
    data in the same municipality as the polling station.

5.  Compute the string distance between the address of polling stations
    and address of schools in INPE and 2010 CNEFE data.

6.  Compute the string distance between the street name and neighborhood
    name of the polling station and street and neighborhood names from
    the CNEFE datasets.

These are very common, yet not used consistently and as a result, are
relatively uninformative.

We found that removing them improves matching performance.

The string matching procedure above generates 8 different potential
matches.

### Choosing Among Potential Matches

After string matching, we use a Random Forest model to predict the
distance between the possible coordinates and the true coordinates. We
treat the coordinates provided by the election authorities as the
“ground truth”. This distance is modeled as a function of the following
set of covariates:

-   Normalized Levenshtein string distance.
-   Coordinate data source
-   Indicator for whether the address mentions the city center
    (“centro”)
-   Indicator for whether the address mentions being in the countryside
    (includes the word “rural”)
-   Indicator for whether the address mentions a school
-   Log of municipal population
-   Proportion of the population classified as rural
-   Area of the municipality

We use the implementation of the Random Forest model provided in the
[`ranger`](https://cran.r-project.org/web/packages/ranger/ranger.pdf)
package. We set the number of trees to 1000 and use five-fold
cross-validation to select the number of variables to use in each tree
and the minimum number of units in each leaf node. After tuning, we
train the model on all polling stations with ground truth coordinates.
We then use this model to predict the distance between the true
coordinates and the candidate coordinates. For each polling station, we
choose the candidate coordinate with the smallest predicted distance.

### Example of String Matching

To illustrate the string matching procedure, the table below shows shows
the string matching procedure for one polling station where the
coordinates are known. “String distance” is the normalized Levenshtein
string distance between the address component and its potential match.
“Predicted distance” is the distance from the truth predicted by the
Random forest model. The blue row shows the selected match, which is the
potential match with the smallest predicted distance. The last column
labeled “Error (km)” is the difference between the known geocoded
coordinates and the coordinates from the selected match.

<style>html {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
}

#ruvibgecyb .gt_table {
  display: table;
  border-collapse: collapse;
  margin-left: auto;
  margin-right: auto;
  color: #333333;
  font-size: 16px;
  font-weight: normal;
  font-style: normal;
  background-color: #FFFFFF;
  width: auto;
  border-top-style: solid;
  border-top-width: 2px;
  border-top-color: #A8A8A8;
  border-right-style: none;
  border-right-width: 2px;
  border-right-color: #D3D3D3;
  border-bottom-style: solid;
  border-bottom-width: 2px;
  border-bottom-color: #A8A8A8;
  border-left-style: none;
  border-left-width: 2px;
  border-left-color: #D3D3D3;
}

#ruvibgecyb .gt_heading {
  background-color: #FFFFFF;
  text-align: center;
  border-bottom-color: #FFFFFF;
  border-left-style: none;
  border-left-width: 1px;
  border-left-color: #D3D3D3;
  border-right-style: none;
  border-right-width: 1px;
  border-right-color: #D3D3D3;
}

#ruvibgecyb .gt_title {
  color: #333333;
  font-size: 125%;
  font-weight: initial;
  padding-top: 4px;
  padding-bottom: 4px;
  border-bottom-color: #FFFFFF;
  border-bottom-width: 0;
}

#ruvibgecyb .gt_subtitle {
  color: #333333;
  font-size: 85%;
  font-weight: initial;
  padding-top: 0;
  padding-bottom: 4px;
  border-top-color: #FFFFFF;
  border-top-width: 0;
}

#ruvibgecyb .gt_bottom_border {
  border-bottom-style: solid;
  border-bottom-width: 2px;
  border-bottom-color: #D3D3D3;
}

#ruvibgecyb .gt_col_headings {
  border-top-style: solid;
  border-top-width: 2px;
  border-top-color: #D3D3D3;
  border-bottom-style: solid;
  border-bottom-width: 2px;
  border-bottom-color: #D3D3D3;
  border-left-style: none;
  border-left-width: 1px;
  border-left-color: #D3D3D3;
  border-right-style: none;
  border-right-width: 1px;
  border-right-color: #D3D3D3;
}

#ruvibgecyb .gt_col_heading {
  color: #333333;
  background-color: #FFFFFF;
  font-size: 100%;
  font-weight: normal;
  text-transform: inherit;
  border-left-style: none;
  border-left-width: 1px;
  border-left-color: #D3D3D3;
  border-right-style: none;
  border-right-width: 1px;
  border-right-color: #D3D3D3;
  vertical-align: bottom;
  padding-top: 5px;
  padding-bottom: 6px;
  padding-left: 5px;
  padding-right: 5px;
  overflow-x: hidden;
}

#ruvibgecyb .gt_column_spanner_outer {
  color: #333333;
  background-color: #FFFFFF;
  font-size: 100%;
  font-weight: normal;
  text-transform: inherit;
  padding-top: 0;
  padding-bottom: 0;
  padding-left: 4px;
  padding-right: 4px;
}

#ruvibgecyb .gt_column_spanner_outer:first-child {
  padding-left: 0;
}

#ruvibgecyb .gt_column_spanner_outer:last-child {
  padding-right: 0;
}

#ruvibgecyb .gt_column_spanner {
  border-bottom-style: solid;
  border-bottom-width: 2px;
  border-bottom-color: #D3D3D3;
  vertical-align: bottom;
  padding-top: 5px;
  padding-bottom: 6px;
  overflow-x: hidden;
  display: inline-block;
  width: 100%;
}

#ruvibgecyb .gt_group_heading {
  padding: 8px;
  color: #333333;
  background-color: #FFFFFF;
  font-size: 100%;
  font-weight: initial;
  text-transform: inherit;
  border-top-style: solid;
  border-top-width: 2px;
  border-top-color: #D3D3D3;
  border-bottom-style: solid;
  border-bottom-width: 2px;
  border-bottom-color: #D3D3D3;
  border-left-style: none;
  border-left-width: 1px;
  border-left-color: #D3D3D3;
  border-right-style: none;
  border-right-width: 1px;
  border-right-color: #D3D3D3;
  vertical-align: middle;
}

#ruvibgecyb .gt_empty_group_heading {
  padding: 0.5px;
  color: #333333;
  background-color: #FFFFFF;
  font-size: 100%;
  font-weight: initial;
  border-top-style: solid;
  border-top-width: 2px;
  border-top-color: #D3D3D3;
  border-bottom-style: solid;
  border-bottom-width: 2px;
  border-bottom-color: #D3D3D3;
  vertical-align: middle;
}

#ruvibgecyb .gt_from_md > :first-child {
  margin-top: 0;
}

#ruvibgecyb .gt_from_md > :last-child {
  margin-bottom: 0;
}

#ruvibgecyb .gt_row {
  padding-top: 8px;
  padding-bottom: 8px;
  padding-left: 5px;
  padding-right: 5px;
  margin: 10px;
  border-top-style: solid;
  border-top-width: 1px;
  border-top-color: #D3D3D3;
  border-left-style: none;
  border-left-width: 1px;
  border-left-color: #D3D3D3;
  border-right-style: none;
  border-right-width: 1px;
  border-right-color: #D3D3D3;
  vertical-align: middle;
  overflow-x: hidden;
}

#ruvibgecyb .gt_stub {
  color: #333333;
  background-color: #FFFFFF;
  font-size: 100%;
  font-weight: initial;
  text-transform: inherit;
  border-right-style: solid;
  border-right-width: 2px;
  border-right-color: #D3D3D3;
  padding-left: 12px;
}

#ruvibgecyb .gt_summary_row {
  color: #333333;
  background-color: #FFFFFF;
  text-transform: inherit;
  padding-top: 8px;
  padding-bottom: 8px;
  padding-left: 5px;
  padding-right: 5px;
}

#ruvibgecyb .gt_first_summary_row {
  padding-top: 8px;
  padding-bottom: 8px;
  padding-left: 5px;
  padding-right: 5px;
  border-top-style: solid;
  border-top-width: 2px;
  border-top-color: #D3D3D3;
}

#ruvibgecyb .gt_grand_summary_row {
  color: #333333;
  background-color: #FFFFFF;
  text-transform: inherit;
  padding-top: 8px;
  padding-bottom: 8px;
  padding-left: 5px;
  padding-right: 5px;
}

#ruvibgecyb .gt_first_grand_summary_row {
  padding-top: 8px;
  padding-bottom: 8px;
  padding-left: 5px;
  padding-right: 5px;
  border-top-style: double;
  border-top-width: 6px;
  border-top-color: #D3D3D3;
}

#ruvibgecyb .gt_striped {
  background-color: rgba(128, 128, 128, 0.05);
}

#ruvibgecyb .gt_table_body {
  border-top-style: solid;
  border-top-width: 2px;
  border-top-color: #D3D3D3;
  border-bottom-style: solid;
  border-bottom-width: 2px;
  border-bottom-color: #D3D3D3;
}

#ruvibgecyb .gt_footnotes {
  color: #333333;
  background-color: #FFFFFF;
  border-bottom-style: none;
  border-bottom-width: 2px;
  border-bottom-color: #D3D3D3;
  border-left-style: none;
  border-left-width: 2px;
  border-left-color: #D3D3D3;
  border-right-style: none;
  border-right-width: 2px;
  border-right-color: #D3D3D3;
}

#ruvibgecyb .gt_footnote {
  margin: 0px;
  font-size: 90%;
  padding: 4px;
}

#ruvibgecyb .gt_sourcenotes {
  color: #333333;
  background-color: #FFFFFF;
  border-bottom-style: none;
  border-bottom-width: 2px;
  border-bottom-color: #D3D3D3;
  border-left-style: none;
  border-left-width: 2px;
  border-left-color: #D3D3D3;
  border-right-style: none;
  border-right-width: 2px;
  border-right-color: #D3D3D3;
}

#ruvibgecyb .gt_sourcenote {
  font-size: 90%;
  padding: 4px;
}

#ruvibgecyb .gt_left {
  text-align: left;
}

#ruvibgecyb .gt_center {
  text-align: center;
}

#ruvibgecyb .gt_right {
  text-align: right;
  font-variant-numeric: tabular-nums;
}

#ruvibgecyb .gt_font_normal {
  font-weight: normal;
}

#ruvibgecyb .gt_font_bold {
  font-weight: bold;
}

#ruvibgecyb .gt_font_italic {
  font-style: italic;
}

#ruvibgecyb .gt_super {
  font-size: 65%;
}

#ruvibgecyb .gt_footnote_marks {
  font-style: italic;
  font-size: 65%;
}
</style>
<div id="ruvibgecyb" style="overflow-x:auto;overflow-y:auto;width:auto;height:auto;"><table class="gt_table">
  <thead class="gt_header">
    <tr>
      <th colspan="6" class="gt_heading gt_title gt_font_normal" style>Example of String Matching</th>
    </tr>
    <tr>
      <th colspan="6" class="gt_heading gt_subtitle gt_font_normal gt_bottom_border" style>Polling Station Name is COLEGIO SCALABRINIANO SAO JOSE. Polling Station Address is RUA ELIZEU ORLANDINI, 115</th>
    </tr>
  </thead>
  <thead class="gt_col_headings">
    <tr>
      <th class="gt_col_heading gt_columns_bottom_border gt_left" rowspan="1" colspan="1">Data</th>
      <th class="gt_col_heading gt_columns_bottom_border gt_left" rowspan="1" colspan="1">Polling Station String</th>
      <th class="gt_col_heading gt_columns_bottom_border gt_left" rowspan="1" colspan="1">Match</th>
      <th class="gt_col_heading gt_columns_bottom_border gt_right" rowspan="1" colspan="1">String Distance</th>
      <th class="gt_col_heading gt_columns_bottom_border gt_right" rowspan="1" colspan="1">Predicted Error (km)</th>
      <th class="gt_col_heading gt_columns_bottom_border gt_right" rowspan="1" colspan="1">True Error (km)</th>
    </tr>
  </thead>
  <tbody class="gt_table_body">
    <tr>
      <td class="gt_row gt_left">INEP School Name</td>
      <td class="gt_row gt_left">scalabriniano sao jose</td>
      <td class="gt_row gt_left">scalabriniano sao jose</td>
      <td class="gt_row gt_right">0.00</td>
      <td class="gt_row gt_right">0.18</td>
      <td class="gt_row gt_right">0.04</td>
    </tr>
    <tr>
      <td class="gt_row gt_left" style="background-color: #E0FFFF;">INEP School Address</td>
      <td class="gt_row gt_left" style="background-color: #E0FFFF;">rua elizeu orlandini 115 centro</td>
      <td class="gt_row gt_left" style="background-color: #E0FFFF;">rua eliseu orlandini 115 centro</td>
      <td class="gt_row gt_right" style="background-color: #E0FFFF;">0.03</td>
      <td class="gt_row gt_right" style="background-color: #E0FFFF;">0.08</td>
      <td class="gt_row gt_right" style="background-color: #E0FFFF;">0.04</td>
    </tr>
    <tr>
      <td class="gt_row gt_left">2010 CNEFE School Name</td>
      <td class="gt_row gt_left">scalabriniano sao jose</td>
      <td class="gt_row gt_left">sinodal</td>
      <td class="gt_row gt_right">0.77</td>
      <td class="gt_row gt_right">4.83</td>
      <td class="gt_row gt_right">0.46</td>
    </tr>
    <tr>
      <td class="gt_row gt_left">2010 CNEFE School Address</td>
      <td class="gt_row gt_left">rua elizeu orlandini 115 centro</td>
      <td class="gt_row gt_left">rua eliseu orlandini 115</td>
      <td class="gt_row gt_right">0.26</td>
      <td class="gt_row gt_right">0.59</td>
      <td class="gt_row gt_right">0.10</td>
    </tr>
    <tr>
      <td class="gt_row gt_left">2017 CNEFE Street</td>
      <td class="gt_row gt_left">rua elizeu orlandini 115</td>
      <td class="gt_row gt_left">rua 21 de abril</td>
      <td class="gt_row gt_right">0.67</td>
      <td class="gt_row gt_right">5.22</td>
      <td class="gt_row gt_right">2.17</td>
    </tr>
    <tr>
      <td class="gt_row gt_left">2010 CNEFE Street</td>
      <td class="gt_row gt_left">rua elizeu orlandini 115</td>
      <td class="gt_row gt_left">rua eliseu orlandini</td>
      <td class="gt_row gt_right">0.21</td>
      <td class="gt_row gt_right">0.40</td>
      <td class="gt_row gt_right">0.10</td>
    </tr>
    <tr>
      <td class="gt_row gt_left">2017 CNEFE Neighborhood</td>
      <td class="gt_row gt_left">centro</td>
      <td class="gt_row gt_left">interior</td>
      <td class="gt_row gt_right">0.62</td>
      <td class="gt_row gt_right">4.51</td>
      <td class="gt_row gt_right">2.77</td>
    </tr>
    <tr>
      <td class="gt_row gt_left">2010 CNEFE Neighborhood</td>
      <td class="gt_row gt_left">centro</td>
      <td class="gt_row gt_left">centro</td>
      <td class="gt_row gt_right">0.00</td>
      <td class="gt_row gt_right">0.74</td>
      <td class="gt_row gt_right">0.78</td>
    </tr>
  </tbody>
  <tfoot class="gt_sourcenotes">
    <tr>
      <td class="gt_sourcenote" colspan="6">Highlighted row is selected match.</td>
    </tr>
  </tfoot>
  
</table></div>

## Estimating Geocoding Error

To estimate the accuracy of our procedure, we use a subset of 23,396
with coordinates to check the error of the coordinates generated by our
procedure. These “ground truth” coordinates were provided to the *Estado
de Sāo Paulo* by the TSE, which used this data for a story entitled
“[Como votou sua
vizinhança?](https://www.estadao.com.br/infograficos/politica,como-votou-sua-vizinhanca-explore-o-mapa-mais-detalhado-das-eleicoes,935858)”.
We report the error rate at 3 quantiles: 25th, 50th (the median), and
the 75th percentile.

In addition to reporting the error rate for the full sample, we also
split the sample into polling stations located in rural census tracts
versus urban census tracts. Finally, we compare our error rate to
coordinates generated using the Google Maps
[API](https://developers.google.com/maps/documentation/geocoding/overview).

<style>html {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
}

#hdemafnejf .gt_table {
  display: table;
  border-collapse: collapse;
  margin-left: auto;
  margin-right: auto;
  color: #333333;
  font-size: 16px;
  font-weight: normal;
  font-style: normal;
  background-color: #FFFFFF;
  width: auto;
  border-top-style: solid;
  border-top-width: 2px;
  border-top-color: #A8A8A8;
  border-right-style: none;
  border-right-width: 2px;
  border-right-color: #D3D3D3;
  border-bottom-style: solid;
  border-bottom-width: 2px;
  border-bottom-color: #A8A8A8;
  border-left-style: none;
  border-left-width: 2px;
  border-left-color: #D3D3D3;
}

#hdemafnejf .gt_heading {
  background-color: #FFFFFF;
  text-align: center;
  border-bottom-color: #FFFFFF;
  border-left-style: none;
  border-left-width: 1px;
  border-left-color: #D3D3D3;
  border-right-style: none;
  border-right-width: 1px;
  border-right-color: #D3D3D3;
}

#hdemafnejf .gt_title {
  color: #333333;
  font-size: 125%;
  font-weight: initial;
  padding-top: 4px;
  padding-bottom: 4px;
  border-bottom-color: #FFFFFF;
  border-bottom-width: 0;
}

#hdemafnejf .gt_subtitle {
  color: #333333;
  font-size: 85%;
  font-weight: initial;
  padding-top: 0;
  padding-bottom: 4px;
  border-top-color: #FFFFFF;
  border-top-width: 0;
}

#hdemafnejf .gt_bottom_border {
  border-bottom-style: solid;
  border-bottom-width: 2px;
  border-bottom-color: #D3D3D3;
}

#hdemafnejf .gt_col_headings {
  border-top-style: solid;
  border-top-width: 2px;
  border-top-color: #D3D3D3;
  border-bottom-style: solid;
  border-bottom-width: 2px;
  border-bottom-color: #D3D3D3;
  border-left-style: none;
  border-left-width: 1px;
  border-left-color: #D3D3D3;
  border-right-style: none;
  border-right-width: 1px;
  border-right-color: #D3D3D3;
}

#hdemafnejf .gt_col_heading {
  color: #333333;
  background-color: #FFFFFF;
  font-size: 100%;
  font-weight: normal;
  text-transform: inherit;
  border-left-style: none;
  border-left-width: 1px;
  border-left-color: #D3D3D3;
  border-right-style: none;
  border-right-width: 1px;
  border-right-color: #D3D3D3;
  vertical-align: bottom;
  padding-top: 5px;
  padding-bottom: 6px;
  padding-left: 5px;
  padding-right: 5px;
  overflow-x: hidden;
}

#hdemafnejf .gt_column_spanner_outer {
  color: #333333;
  background-color: #FFFFFF;
  font-size: 100%;
  font-weight: normal;
  text-transform: inherit;
  padding-top: 0;
  padding-bottom: 0;
  padding-left: 4px;
  padding-right: 4px;
}

#hdemafnejf .gt_column_spanner_outer:first-child {
  padding-left: 0;
}

#hdemafnejf .gt_column_spanner_outer:last-child {
  padding-right: 0;
}

#hdemafnejf .gt_column_spanner {
  border-bottom-style: solid;
  border-bottom-width: 2px;
  border-bottom-color: #D3D3D3;
  vertical-align: bottom;
  padding-top: 5px;
  padding-bottom: 6px;
  overflow-x: hidden;
  display: inline-block;
  width: 100%;
}

#hdemafnejf .gt_group_heading {
  padding: 8px;
  color: #333333;
  background-color: #FFFFFF;
  font-size: 100%;
  font-weight: initial;
  text-transform: inherit;
  border-top-style: solid;
  border-top-width: 2px;
  border-top-color: #D3D3D3;
  border-bottom-style: solid;
  border-bottom-width: 2px;
  border-bottom-color: #D3D3D3;
  border-left-style: none;
  border-left-width: 1px;
  border-left-color: #D3D3D3;
  border-right-style: none;
  border-right-width: 1px;
  border-right-color: #D3D3D3;
  vertical-align: middle;
}

#hdemafnejf .gt_empty_group_heading {
  padding: 0.5px;
  color: #333333;
  background-color: #FFFFFF;
  font-size: 100%;
  font-weight: initial;
  border-top-style: solid;
  border-top-width: 2px;
  border-top-color: #D3D3D3;
  border-bottom-style: solid;
  border-bottom-width: 2px;
  border-bottom-color: #D3D3D3;
  vertical-align: middle;
}

#hdemafnejf .gt_from_md > :first-child {
  margin-top: 0;
}

#hdemafnejf .gt_from_md > :last-child {
  margin-bottom: 0;
}

#hdemafnejf .gt_row {
  padding-top: 8px;
  padding-bottom: 8px;
  padding-left: 5px;
  padding-right: 5px;
  margin: 10px;
  border-top-style: solid;
  border-top-width: 1px;
  border-top-color: #D3D3D3;
  border-left-style: none;
  border-left-width: 1px;
  border-left-color: #D3D3D3;
  border-right-style: none;
  border-right-width: 1px;
  border-right-color: #D3D3D3;
  vertical-align: middle;
  overflow-x: hidden;
}

#hdemafnejf .gt_stub {
  color: #333333;
  background-color: #FFFFFF;
  font-size: 100%;
  font-weight: initial;
  text-transform: inherit;
  border-right-style: solid;
  border-right-width: 2px;
  border-right-color: #D3D3D3;
  padding-left: 12px;
}

#hdemafnejf .gt_summary_row {
  color: #333333;
  background-color: #FFFFFF;
  text-transform: inherit;
  padding-top: 8px;
  padding-bottom: 8px;
  padding-left: 5px;
  padding-right: 5px;
}

#hdemafnejf .gt_first_summary_row {
  padding-top: 8px;
  padding-bottom: 8px;
  padding-left: 5px;
  padding-right: 5px;
  border-top-style: solid;
  border-top-width: 2px;
  border-top-color: #D3D3D3;
}

#hdemafnejf .gt_grand_summary_row {
  color: #333333;
  background-color: #FFFFFF;
  text-transform: inherit;
  padding-top: 8px;
  padding-bottom: 8px;
  padding-left: 5px;
  padding-right: 5px;
}

#hdemafnejf .gt_first_grand_summary_row {
  padding-top: 8px;
  padding-bottom: 8px;
  padding-left: 5px;
  padding-right: 5px;
  border-top-style: double;
  border-top-width: 6px;
  border-top-color: #D3D3D3;
}

#hdemafnejf .gt_striped {
  background-color: rgba(128, 128, 128, 0.05);
}

#hdemafnejf .gt_table_body {
  border-top-style: solid;
  border-top-width: 2px;
  border-top-color: #D3D3D3;
  border-bottom-style: solid;
  border-bottom-width: 2px;
  border-bottom-color: #D3D3D3;
}

#hdemafnejf .gt_footnotes {
  color: #333333;
  background-color: #FFFFFF;
  border-bottom-style: none;
  border-bottom-width: 2px;
  border-bottom-color: #D3D3D3;
  border-left-style: none;
  border-left-width: 2px;
  border-left-color: #D3D3D3;
  border-right-style: none;
  border-right-width: 2px;
  border-right-color: #D3D3D3;
}

#hdemafnejf .gt_footnote {
  margin: 0px;
  font-size: 90%;
  padding: 4px;
}

#hdemafnejf .gt_sourcenotes {
  color: #333333;
  background-color: #FFFFFF;
  border-bottom-style: none;
  border-bottom-width: 2px;
  border-bottom-color: #D3D3D3;
  border-left-style: none;
  border-left-width: 2px;
  border-left-color: #D3D3D3;
  border-right-style: none;
  border-right-width: 2px;
  border-right-color: #D3D3D3;
}

#hdemafnejf .gt_sourcenote {
  font-size: 90%;
  padding: 4px;
}

#hdemafnejf .gt_left {
  text-align: left;
}

#hdemafnejf .gt_center {
  text-align: center;
}

#hdemafnejf .gt_right {
  text-align: right;
  font-variant-numeric: tabular-nums;
}

#hdemafnejf .gt_font_normal {
  font-weight: normal;
}

#hdemafnejf .gt_font_bold {
  font-weight: bold;
}

#hdemafnejf .gt_font_italic {
  font-style: italic;
}

#hdemafnejf .gt_super {
  font-size: 65%;
}

#hdemafnejf .gt_footnote_marks {
  font-style: italic;
  font-size: 65%;
}
</style>
<div id="hdemafnejf" style="overflow-x:auto;overflow-y:auto;width:auto;height:auto;"><table class="gt_table">
  <thead class="gt_header">
    <tr>
      <th colspan="7" class="gt_heading gt_title gt_font_normal" style>Geocoding Error</th>
    </tr>
    <tr>
      <th colspan="7" class="gt_heading gt_subtitle gt_font_normal gt_bottom_border" style></th>
    </tr>
  </thead>
  <thead class="gt_col_headings">
    <tr>
      <th class="gt_col_heading gt_center gt_columns_bottom_border" rowspan="2" colspan="1">Quantile</th>
      <th class="gt_center gt_columns_top_border gt_column_spanner_outer" rowspan="1" colspan="3">
        <span class="gt_column_spanner">String Matching</span>
      </th>
      <th class="gt_center gt_columns_top_border gt_column_spanner_outer" rowspan="1" colspan="3">
        <span class="gt_column_spanner">Google Maps</span>
      </th>
    </tr>
    <tr>
      <th class="gt_col_heading gt_columns_bottom_border gt_center" rowspan="1" colspan="1">Full Sample</th>
      <th class="gt_col_heading gt_columns_bottom_border gt_center" rowspan="1" colspan="1">Urban</th>
      <th class="gt_col_heading gt_columns_bottom_border gt_center" rowspan="1" colspan="1">Rural</th>
      <th class="gt_col_heading gt_columns_bottom_border gt_center" rowspan="1" colspan="1">Full Sample</th>
      <th class="gt_col_heading gt_columns_bottom_border gt_center" rowspan="1" colspan="1">Urban</th>
      <th class="gt_col_heading gt_columns_bottom_border gt_center" rowspan="1" colspan="1">Rural</th>
    </tr>
  </thead>
  <tbody class="gt_table_body">
    <tr>
      <td class="gt_row gt_left">25th</td>
      <td class="gt_row gt_right">0.03</td>
      <td class="gt_row gt_right">0.03</td>
      <td class="gt_row gt_right">0.08</td>
      <td class="gt_row gt_right">0.05</td>
      <td class="gt_row gt_right">0.04</td>
      <td class="gt_row gt_right">0.82</td>
    </tr>
    <tr>
      <td class="gt_row gt_left">Median</td>
      <td class="gt_row gt_right">0.17</td>
      <td class="gt_row gt_right">0.10</td>
      <td class="gt_row gt_right">0.61</td>
      <td class="gt_row gt_right">0.30</td>
      <td class="gt_row gt_right">0.15</td>
      <td class="gt_row gt_right">6.21</td>
    </tr>
    <tr>
      <td class="gt_row gt_left">75th</td>
      <td class="gt_row gt_right">0.75</td>
      <td class="gt_row gt_right">0.43</td>
      <td class="gt_row gt_right">2.80</td>
      <td class="gt_row gt_right">3.77</td>
      <td class="gt_row gt_right">0.56</td>
      <td class="gt_row gt_right">18.46</td>
    </tr>
  </tbody>
  <tfoot class="gt_sourcenotes">
    <tr>
      <td class="gt_sourcenote" colspan="7">Error in kilometers.</td>
    </tr>
  </tfoot>
  
</table></div>

As can be seen in the above table, the median error for our method is
about 0.17 km. This median error is *lower* than the median error of
coordinates produced by the Google Maps Geocoding API, which is 0.3 km.

When we separate the sample by rural or urban, we see that our method is
more accurate for both types of polling station. The difference is
particularly large for rural polling stations, where our median error
rate is 0.61 km and the Google median error rate is 6.21 km, about a 10
fold difference.

[1] Details on the 2017 CNEFE can be found at this
[link](https://biblioteca.ibge.gov.br/visualizacao/livros/liv101638_notas_tecnicas.pdf)

[2] The data can be found at this
[link](https://inepdata.inep.gov.br/analytics/saw.dll?dashboard)

[3] We remove common, but uninformative words, such as “povado” and
“localidade”. We standardize common street abbreviations such as
replacing “Av” with “Avenida”. Finally, for polling station names, we
remove words most common in school names, such as “unidade escolar” and
“colegio estadual”.
