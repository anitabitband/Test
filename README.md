
# yoink: data-fetcher for the AAT/PPI

This is an implementation of the NRAO archive's Java-based data-fetcher in Python, as a research experiment on how it could be modularized and re-structured.
 
Use cases yoink is intended to handle:
   * works for both science products and ancillary products
   * download a product from the NRAO archive by specifying its product locator
   * download a product from the NRAO archive by providing a path to a locator report
   * streaming or direct copy downloads based on file location and execution site
   
This is intended to be a library wrapped in a command line interface.
 
```
```