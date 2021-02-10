"""Small, shared functions used to generate inputs and parameters.
"""
import datetime


def numeric_date(dt=None):
    """
    Convert datetime object to the numeric date.
    The numeric date format is YYYY.F, where F is the fraction of the year passed
    Parameters
    ----------
     dt:  datetime.datetime, None
        date of to be converted. if None, assume today
    """
    from calendar import isleap

    if dt is None:
        dt = datetime.datetime.now()

    days_in_year = 366 if isleap(dt.year) else 365
    try:
        res = dt.year + (dt.timetuple().tm_yday-0.5) / days_in_year
    except:
        res = None

    return res

def _get_subsampling_scheme_by_build_name(build_name):
    return config["builds"][build_name].get("subsampling_scheme", build_name)

def _get_path_for_input(stage, origin_wildcard):
    """
    Inputs may define a local filepath, or a s3:// address. This function allows
    snakemake to call a separate rule to download the remote resource, if needed.
    """
    origin = origin_wildcard[1:] # trim leading `_`
    value = config.get("inputs", {}).get(origin, {}).get(stage, "")
    on_s3 = value.startswith("s3://")

    ## Basic checking which could be taken care of by the config schema
    if not value and value in ["metadata", "sequences"]:
        raise Exception(f"ERROR: config->input->{origin}->{stage} is not defined.")

    if stage=="metadata":
        return f"data/downloaded{origin_wildcard}.tsv" if on_s3 else value
    if stage=="sequences":
        return f"data/downloaded{origin_wildcard}.fasta" if on_s3 else value
    if stage=="filtered":
        return f"results/precomputed-filtered{origin_wildcard}.fasta" if on_s3 else f"results/filtered{origin_wildcard}.fasta"

    raise Exception(f"_get_path_for_input with unknown stage \"{stage}\"")


def _get_single_metadata(wildcards):
    """If multiple origins are specified, we fetch the metadata corresponding
    to that origin. If multiple inputs are employed, we simply return the
    corresponding (un-modified) metadata input
    """
    if wildcards["origin"] == "":
        if "inputs" in config:
            raise Exception("WARNING: empty origin wildcard but config defines 'inputs`")
        return config["metadata"]
    return _get_path_for_input("metadata", wildcards.origin)


def _get_unified_metadata(wildcards):
    """Returns a singular metadata file representing the input metadata file(s).
    If a single input was specified, this is returned.
    If multiple inputs are specified, a rule to combine them is used.
    """
    if "inputs" in config:
        return "results/combined_metadata.tsv"  # see rule combine_input_metadata
    if config['metadata'].startswith("s3://"):
        return "data"
    return config['metadata']

def _get_metadata_by_build_name(build_name):
    """Returns a path associated with the metadata for the given build name.

    The path can include wildcards that must be provided by the caller through
    the Snakemake `expand` function or through string formatting with `.format`.
    """
    if build_name == "global" or "region" not in config["builds"][build_name]:
        return _get_unified_metadata({})
    else:
        return rules.adjust_metadata_regions.output.metadata

def _get_metadata_by_wildcards(wildcards):
    """Returns a metadata path based on the given wildcards object.

    This function is designed to be used as an input function.
    """
    return _get_metadata_by_build_name(wildcards.build_name)

def _get_sampling_trait_for_wildcards(wildcards):
    if wildcards.build_name in config["exposure"]:
        return config["exposure"][wildcards.build_name]["trait"]
    else:
        return config["exposure"]["default"]["trait"]

def _get_exposure_trait_for_wildcards(wildcards):
    if wildcards.build_name in config["exposure"]:
        return config["exposure"][wildcards.build_name]["exposure"]
    else:
        return config["exposure"]["default"]["exposure"]

def _get_trait_columns_by_wildcards(wildcards):
    if wildcards.build_name in config["traits"]:
        return config["traits"][wildcards.build_name]["columns"]
    else:
        return config["traits"]["default"]["columns"]

def _get_sampling_bias_correction_for_wildcards(wildcards):
    if wildcards.build_name in config["traits"] and 'sampling_bias_correction' in config["traits"][wildcards.build_name]:
        return config["traits"][wildcards.build_name]["sampling_bias_correction"]
    else:
        return config["traits"]["default"]["sampling_bias_correction"]

def _get_max_date_for_frequencies(wildcards):
    if "frequencies" in config and "max_date" in config["frequencies"]:
        return config["frequencies"]["max_date"]
    else:
        return numeric_date(date.today())

def _get_first(config, *keys):
    """
    Get the value of the first key in *keys* that exists in *config* and has a
    non-empty value.

    Raises a :class:`KeyError` if none of the *keys* exist with a suitable
    value.
    """
    for key in keys:
        if config.get(key) not in {"", None}:
            return config[key]
    raise KeyError(str(keys))
