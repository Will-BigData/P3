### 1: Total Population by Race Across the United States (2000–2020)

#### **Overview**
This analysis explores the total population by race across the United States, focusing on changes from 2000 to 2020. The analysis includes percentage changes in population for each race at the state level over the three decades.
## Steps followed in the analysis
### select Columns of Interest
 - **`P0010003` to `P0010009`:** Represent populations by specific racial categories:
### Filter state level data
 - **`SUMLEV == 40`:** Filters the data to include only state-level information.
### Aggregate the total population by race for each state.
```python
total_population_by_state = state_level_df.groupBy("STUSAB").agg(
        sum("P0010003").alias("White_Alone"),
        sum("P0010004").alias("Black_Alone"),
        sum("P0010005").alias("American_Indian_Alone"),
        sum("P0010006").alias("Asian_Alone"),
        sum("P0010007").alias("Native_Hawaiian_Alone"),
        sum("P0010008").alias("Some_Other_Race"),
        sum("P0010009").alias("Two_or_More_Races")
    )
```
## Sample code
![percentage change](/P3/dejen_questions_analysis/images/Q1Races.png)

## Spark Output loaded into power bi
![Power BI](/P3/dejen_questions_analysis/images/Q1PB2.png)

# Visualizations
![Total Population](/P3/dejen_questions_analysis/images/Q1PB-V.png)


1. **Total Population Trends**
   - The overall population in the U.S. has increased steadily from 2000 to 2020.
   - States with significant population growth include **Texas, Florida, and California**.
   - Some states, such as **West Virginia and Puerto Rico**, experienced population declines.

2. **Racial Population Distribution**
   - **White Alone**: The largest population group, but its proportion has decreased in many states due to slower growth compared to other racial groups.
   - **Black Alone**: Notable growth in states like Georgia, North Carolina, and Texas.
   - **Asian Alone**: The fastest-growing racial group, particularly in states such as California, Texas, and New York.
   - **Two or More Races**: Significant growth across almost all states, with some experiencing over 200% growth (e.g., California, Florida).

3. **Percentage Changes by Race**
   - **American Indian Alone**: Growth concentrated in states with large tribal populations (e.g., Arizona, New Mexico, and Oklahoma).
   - **Native Hawaiian Alone**: Growth primarily in Hawaii and other western states.
   - **Some Other Race**: A sharp increase in states in Texas and Arizona.

- **State Outliers**:
  - **California**: While it remains the most populous state, its growth rate has slowed significantly compared to the previous decade.
  - **Texas**: Continued strong growth in all racial groups, particularly Hispanic and Asian populations.
  - **Hawaii**: Significant growth in Native Hawaiian and Pacific Islander populations.

- **Racial Shifts**:
  - The White Alone population, while still the majority in most states, is shrinking in proportion as other racial groups grow faster.
  - Multi-racial and minority populations are driving diversity in urban areas.


#### **Conclusion**
The racial composition of the U.S. is changing, driven by growth in minority populations and the diversification of urban areas.