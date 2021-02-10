# Here we define a number of rules which produce the same output as rules in
# the main_workflow, but do so by downloading the files from S3 rather than
# computing them.

# We control which rule runs via `ruleorder` declarations
ruleorder: align > download_aligned
ruleorder: refilter > download_refiltered
ruleorder: mask > download_masked
ruleorder: diagnostic > download_diagnostic


rule download_sequences:
    message: "Downloading sequences from {params.address} -> {output.sequences}"
    output:
        sequences = "data/downloaded{origin}.fasta"
    conda: config["conda_environment"]
    params:
        address = lambda w: config["inputs"][w.origin[1:]]["sequences"]
    shell:
        """
        aws s3 cp {params.address} - | gunzip -cq > {output.sequences:q}
        """

rule download_metadata:
    message: "Downloading metadata from {params.address} -> {output.metadata}"
    output:
        metadata = "data/downloaded{origin}.tsv"
    conda: config["conda_environment"]
    params:
        address = lambda w: config["inputs"][w.origin[1:]]["metadata"]
    shell:
        """
        aws s3 cp {params.address} - | gunzip -cq >{output.metadata:q}
        """

rule download_aligned:
    message: "Downloading aligned fasta files from S3 bucket {params.s3_bucket}"
    output:
        sequences = "results/aligned{origin}.fasta"
    conda: config["conda_environment"]
    params:
        compression = config['preprocess']['compression'],
        deflate = config['preprocess']['deflate'],
        s3_bucket = _get_first(config, "S3_SRC_BUCKET", "S3_BUCKET")
    shell:
        """
        aws s3 cp s3://{params.s3_bucket}/aligned.fasta.{params.compression} - | {params.deflate} > {output.sequences:q}
        """

rule download_diagnostic:
    message: "Downloading diagnostic files from S3 bucket {params.s3_bucket}"
    output:
        diagnostics = "results/sequence-diagnostics.tsv",
        flagged = "results/flagged-sequences.tsv",
        to_exclude = "results/to-exclude.txt"
    conda: config["conda_environment"]
    params:
        compression = config['preprocess']['compression'],
        deflate = config['preprocess']['deflate'],
        s3_bucket = _get_first(config, "S3_SRC_BUCKET", "S3_BUCKET")
    shell:
        """
        aws s3 cp s3://{params.s3_bucket}/sequence-diagnostics.tsv.{params.compression} - | {params.deflate} > {output.diagnostics:q}
        aws s3 cp s3://{params.s3_bucket}/flagged-sequences.tsv.{params.compression} - | {params.deflate} > {output.flagged:q}
        aws s3 cp s3://{params.s3_bucket}/to-exclude.txt.{params.compression} - | {params.deflate} > {output.to_exclude:q}
        """

rule download_refiltered:
    message: "Downloading quality filtered files from S3 bucket {params.s3_bucket}"
    output:
        sequences = "results/aligned-filtered.fasta"
    conda: config["conda_environment"]
    params:
        compression = config['preprocess']['compression'],
        deflate = config['preprocess']['deflate'],
        s3_bucket = _get_first(config, "S3_SRC_BUCKET", "S3_BUCKET")
    shell:
        """
        aws s3 cp s3://{params.s3_bucket}/aligned-filtered.fasta.{params.compression} - | {params.deflate} > {output.sequences:q}
        """


rule download_masked:
    message: "Downloading aligned masked fasta files from S3 bucket {params.s3_bucket}"
    output:
        sequences = "results/masked.fasta"
    conda: config["conda_environment"]
    params:
        compression = config['preprocess']['compression'],
        deflate = config['preprocess']['deflate'],
        s3_bucket = _get_first(config, "S3_SRC_BUCKET", "S3_BUCKET")
    shell:
        """
        aws s3 cp s3://{params.s3_bucket}/masked.fasta.{params.compression} - | {params.deflate} > {output.sequences:q}
        """


rule download_filtered:
    message: "Downloading pre-computed filtered alignment from {params.address} -> {output.sequences}"
    output:
        sequences = "results/precomputed-filtered{origin}.fasta"
    conda: config["conda_environment"]
    params:
        deflate = config['preprocess']['deflate'], ## TODO -- define per input, not in a global config setting
        address = lambda w: config["inputs"][w.origin[1:]]["filtered"]
    shell:
        """
        aws s3 cp {params.address} - | {params.deflate} > {output.sequences:q}
        """