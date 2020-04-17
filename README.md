
# yoink: data-fetcher for the AAT/PPI

This is an implementation of the NRAO archive's Java-based data-fetcher in Python, as a research experiment on how it could be modularized and re-structured.
 
Use cases yoink is intended to handle:
   * works for both science products and ancillary products
   * download a product from the NRAO archive by specifying its product locator
   * download a product from the NRAO archive by providing a path to a locator report
   * streaming or direct copy downloads based on file location and execution site
   
This is intended to be a library wrapped in a command line interface.
 
```
usage: yoink [-h]
             (--product-locator PRODUCT_LOCATOR | --location-file LOCATION_FILE)
             [--dry-run] [--output-dir OUTPUT_DIR] [--sdm-only] [--verbose]
             [--profile PROFILE]

Retrieve a product (a science product or an ancillary product) from the NRAO archive,
either by specifying the product's locator or by providing the path to a product
locator report.

Optional Arguments:
  --dry-run             dry run, do not fetch product
  --output-dir OUTPUT_DIR
                        output directory, default current directory
  --sdm-only            only get the metadata, not the fringes
  --verbose             make a lot of noise
  --profile PROFILE     CAPO profile to use

Return Codes:
	1: no CAPO profile provided
	2: missing required setting
	3: request to locator service timed out
	4: too many redirects on locator service
	5: catastrophic error on request service
	6: product locator not found
	7: not able to open specified location file
	8: error fetching file from NGAS server
	9: retrieved file not expected size
```
