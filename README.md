# MLLM-Housing
First set up your conda environment using the environment.yml file.
Redfin data is downloaded using download_dataset.py (will rename later).
Zillow data is added in the github

Use the MLLM to evaluate the beauty and cleanliness and spaciousness
and other more abstract qualities

## TODO

- [X] Generate tileable blocks
- [X] Implement single tile scraping
- [X] Deal with Pagination
- [X] Validate and parse scraped results (Individual)
- [ ] Implement large scale scraping (scrape all Zillow tiles of interest)
- [ ] Process and extract data
- [ ] Dedupe data (remove duplicate listings/houses)
- [ ] Data cleaning and preprocessing
- [ ] Train initial model on data
- [ ] Test and evaluate model performance
- [ ] Integrate MLLM for evaluation of abstract qualities (beauty, cleanliness, spaciousness, etc.)


## Implementation 

- **Pagination**: Multi-page scraping concatenates `listResults` from each page. Validation checks `page_count >= house_count` (where `house_count` is the total items visible on the map from `mapResults`). 
- **Empty Tiles**: Tiles with zero houses are recorded to `empty_tiles.json` for future skipping.
- **Rate Limiting**: Gaussian-distributed sleep (mean=4s, range 2-10s) before each request to avoid IP blocking.

## Notes
- `page_count` often exceeds `house_count` after pagination, so we've settled on a `>=` check for now. `house_count` is also often different from website pull. website pull `house_count` changes if we paginate