
# yoink: data-fetcher for the AAT/PPI

This is an implementation of the NRAO archive's Java-based data-fetcher in Python, as a research experiment on how it could be modularized and re-structured.
 
Use cases yoink is intended to handle:
   * streaming download of a file of any type from DSOC NGAS by NGAS file_id or AAT file_id
   * direct copy download of a file of any type from DSOC NGAS by NGAS file_id or AAT file_id
   * streaming download of a VLA fileset
   * direct copy download of a VLA fileset
   * streaming downloads of files from NAASC NGAS, somehow
   
This is intended to be a library wrapped in a command line interface.
 
```
usage: yoink [-h] -archive {NRAO_NGAS,NAASC_NGAS} -identifier-type
             {NGAS_file_id,AAT_file_id,AAT_file_group_id,fileset} -identifier
             IDENTIFIERS [IDENTIFIERS ...] [--direct-copy]
             [--output-directory OUTPUT_DIRECTORY] [--bdfs-only | --sdms-only]

Description:
    Fetch files for the NRAO AAT/PPI, from NGAS (streaming or direct copy)
    or maybe eventually by scp or something else, eventually. Delivery to a local
    directory for streaming, an NFS/lustre directory the NGAS servers can see and
    write to as user ngas (for direct copy), or maybe one day to Amazon storage.

Required Arguments:
  -archive {NRAO_NGAS,NAASC_NGAS}
                        archive to fetch the thing from
  -identifier-type {NGAS_file_id,AAT_file_id,AAT_file_group_id,fileset}
                        type of identifier to yoink
  -identifier IDENTIFIERS [IDENTIFIERS ...]
                        value of identifier to yoink

Optional Arguments:
  --direct-copy         direct copy (vs streaming), defaults to False
  --output-directory OUTPUT_DIRECTORY
                        where to put files, defaults to cwd
  --bdfs-only           only download a fileset's BDF files
  --sdms-only           only download a fileset's SDM tables

Examples:
    # Direct copy yoink of a single SDM file from NRAO NGAS to /tmp
    yoink --direct-copy -archive NRAO_NGAS \
        -identifier-type NGAS_file_id \
        -identifier uid___evla_sdm_X1484357856203.sdm \
        --output-directory /tmp

    # Streaming yoink of three EVLA filesets from NRAO NGAS to cwd
    yoink -archive NRAO_NGAS -identifier-type fileset \
        -identifier 18A-095.sb35121219.eb35387982.58246.964106064814 \
            18A-095.sb35122044.eb35396825.58248.989289756944 \
            18A-095.sb35121375.eb35418438.58256.16869506944
```