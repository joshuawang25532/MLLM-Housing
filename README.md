# MLLM-Housing
First set up your conda environment using the environment.yml file
Redfin data is downloaded using download_dataset.py (will rename later)
Zillow data is added in the github

Use the MLLM to evaluate the beauty and cleanliness and spaciousness
and other more abstract qualities

## TODO

- [X] Generate tileable blocks
- [X] Implement single tile scraping
- [ ] Deal with Pagination
- [ ] Validate and parse scraped results (Individual)
- [ ] Implement large scale scraping (scrape all Zillow tiles of interest)
- [ ] Process and extract data
- [ ] Dedupe data (remove duplicate listings/houses)
- [ ] Data cleaning and preprocessing
- [ ] Train initial model on data
- [ ] Test and evaluate model performance
- [ ] Integrate MLLM for evaluation of abstract qualities (beauty, cleanliness, spaciousness, etc.)


Scraped result has a small discrepancy but it is very small so we arent worrying about it