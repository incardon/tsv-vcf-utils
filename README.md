# Tsv Vcf utils

Utilities for Vcf and Tsv files

## Supported Operations

- Merge Vcf,Tsv, or bed files together creating either a vcf or a Varfish compatible table for extra annotation
- Vcf rewrite
- Score server. It launch a service for explore the vcf database online using json format

## Example

### Merger of VCF,TSV and BED files

The following will merge 8 databases into one vcf-file with 57 colums picked from vcf tsv and bed files

```
java -jar tsv-vcf-utils-cli-0.1-jar-with-dependencies.jar init-db --ref-path /nvme/IGSB/nextflow/databases/human_g1k_v37.fasta --format "chr=0:start=1:stop=2:ref=3:alt=4,chr=7:start=8:stop=-1:ref=2:alt=3,chr=0:start=1:stop=-1:ref=3:alt=4,chr=0:start=1:stop=-1:ref=3:alt=4,chr=0:start=1:stop=-1:ref=3:alt=4,chr=0:start=1:stop=-1:ref=2:alt=3,chr=0:start=1:stop=-1:ref=2:alt=3" --tsv-files "/nvme/IGSB/databases/splice_ai_CrossMap_v38tov37_lift_sorted_stripped.vcf_red.gz,/nvme/IGSB/databases/dbNSFP4.1a_test_sorted.gz,/nvme/IGSB/databases/gnomad.genomes.r2.0.1.sites.noVEP.vcf_test.gz,/nvme/IGSB/databases/gnomad.exomes.r2.1.1.sites.vcf_red.bgz,/nvme/IGSB/databases/clinvar.vcf_red.gz,/nvme/IGSB/databases/InDels.tsv_red.gz,/nvme/IGSB/databases/whole_genome_SNVs.tsv_red.gz" --columns-tsv "(7|;|(4|=|(1))):(7|;|(5|=|(1))):(7|;|(6|=|(1))):(7|;|(7|=|(1))),Ensembl_proteinid:MutationTaster_pred:MutationTaster_score:PROVEAN_pred:PROVEAN_score:PrimateAI_pred:PrimateAI_score:Polyphen2_HVAR_pred:SIFT_pred:UK10K_AF:REVEL_score:REVEL_rankscore:phyloP100way_vertebrate:phyloP100way_vertebrate_rankscore:phastCons100way_vertebrate:phastCons100way_vertebrate_rankscore:BayesDel_noAF_score:BayesDel_noAF_pred:gnomAD_genomes_AC:gnomAD_genomes_nhomalt,(7|AF=|(1|;|(0))):(7|AN=|(1|;|(0))):(7|AC=|(1|;|(0))):(7|AF_AFR=|(1|;|(0))):(7|AF_AMR=|(1|;|(0))):(7|AF_ASJ=|(1|;|(0))):(7|AF_EAS=|(1|;|(0))):(7|AF_FIN=|(1|;|(0))):(7|AF_NFE=|(1|;|(0))):(7|AF_OTH=|(1|;|(0))),(7|AF=|(1|;|(0))):(7|AN=|(1|;|(0))):(7|AC=|(1|;|(0))):(7|AF_afr=|(1|;|(0))):(7|AF_amr=|(1|;|(0))):(7|AF_asj=|(1|;|(0))):(7|AF_eas=|(1|;|(0))):(7|AF_fin=|(1|;|(0))):(7|AF_nfe=|(1|;|(0))):(7|AF_oth=|(1|;|(0))):(7|AF_sas=|(1|;|(0))):(7|nhomalt=|(1|;|(0))),(7|CLNSIG=|(1|;|(0))):(7|CLNREVSTAT=|(1|;|(0))):(7|CLNDN=|(1|;|(0))),4:5,4:5" --tsv-fieldname "SpliceAI_DS_AG:SpliceAI_DS_AL:SpliceAI_DS_DG:SpliceAI_DS_DL,Ensembl_proteinid:MutationTaster_pred:MutationTaster_score:PROVEAN_pred:PROVEAN_score:PrimateAI_pred:PrimateAI_score:Polyphen2_HVAR_pred:SIFT_pred:UK10K_AF:REVEL_score:REVEL_rankscore:phyloP100way_vertebrate:phyloP100way_vertebrate_rankscore:phastCons100way_vertebrate:phastCons100way_vertebrate_rankscore:BayesDel_noAF_score:BayesDel_noAF_pred:gnomAD_genomes_AC:gnomAD_genomes_nhomalt,AF:AN:AC:AF_AFR:AF_AMR:AF_ASJ:AF_EAS:AF_FIN:AF_NFE:AF_OTH,AF:AN:AC:AF_afr:AF_amr:AF_asj:AF_eas:AF_fin:AF_nfe:AF_oth:AF_sas:nhomalt,CLNSIG:CLNREVSTAT:CLNDN,CADD_RawScore:CADD_PHRED,CADD_RawScore:CADD_PHRED" --vcf-fieldname "num_index:num_parent:num_parent_not_index" --bed-fieldname "genes_annotation" --bed-files "/nvme/IGSB/databases/genes_annotation.bed.gz" --columns-bed "3" --release GRCh37 --vcf-files "/nvme/IGSB/databases/counts_in_house2019-11-08_fix_sort.vcf.gz" --columns-vcf "num_index:num_parent:num_parent_not_index" --create-vcf-db vcf_database.vcf.bgz --disable-extra-anno
```

Let's crack it down:

init-db is the command

--ref-path /nvme/IGSB/nextflow/databases/human_g1k_v37.fasta is the reference genome in fasta format

--format "chr=0:start=1:stop=2:ref=3:alt=4,chr=7 ...

Indicate for the tsv files in which colums you find the chromosome number, the start position, the stop position, reference, and alternative. In this case chromosome stay at column 0, start at column 1 and so on. For each TSV file there is a format and is saparated by a comma.(For example "...,chr=7 indicates after the comma the format of the second TSV files) 

--tsv-files "/nvme/IGSB/databases/splice_ai_CrossMap_v38tov37_lift_sorted_stripped.vcf_red.gz,/nvme/IGSB/var ...

A list of all TSV files

--columns-tsv "(7|;|(4|=|(1))) ...

Columns selection. For each file (separated by a comma) we select which column. There are several formats

(7|;|(4|=|(1))) is for structures TSV files and it mean. Select Column 7, split the selected column by ";", select the column 4 of the splited text, split the selected by "=" take the column 1 of the splited text.

This give us a lot of flexibility in selecting columns. In particular is a concatenation of select and split to pick the data we want. The next column selection (in the same file) is given by ":". The next file start with ","

When we have like simple numbers 4:5 it means columns 4 and 5

When we have names like SpliceAI_DS_AG:SpliceAI_DS_AL: ... It means that the TSV has a header line at the beginning starting with "#". In this case we can select by name

--vcf-files

List of vcf files to merge. same as --tsv-files

--columns-vcf "num_index:num_parent ...

Is the same as --columns-tsv but in this case you can only use text field and are read from the INFO column. Vcf file must pass the SAM tools quality controls. Note that in case of more complex operations, or malformatted vcf files such files can be read as TSV files. As you notice in the command line some malformatted vcf files are read using the TSV format.

--tsv-fieldname

To each selected columns a name must be assigned. (You can use the original name of the column or reassign one)

--vcf-fieldname

Same concept for --tsv-fieldname for vcf

--bed-fieldname

Same concept for --tsv-fieldname for bed files

--disable-extra-anno

Disable the generation of the extra annotation table.


This tool can be used also to convert TSV into VCF files. Select all or some TSV column, the output will be a calid VCF file with the selected information.

### VCF rewriter fixer

java -jar tsv-vcf-utils-cli-0.1-jar-with-dependencies.jar vcf_rewriter --vcf-in /nvme/case/input/SID106291_AID99591.cmb.func.vcf.gz --vcf-out /nvme/case/output/SID106291_AID99591.cmb.func.vcf.gz --vcf-select-rules "8|\:|[GT],8|\:|[GT]" --vcf-concat-rules "0|\:|[~],0|\:|[~]" --vcf-rewrite-cols "8,9"

This is an example to rewrite and fix VCF files.

The tool is bases on select and rewrite

--vcf-select-rules specify the select rules

Rules are saparated by a ",". So here we have two rules

8|\:|[GT]
8|\:|[GT]

Both specity the same rules and they mean select column 8 split by ":" (note we have to escape "\" the ":", because is used as separator for the Merger), and select the field with name GT

--vcf-concat-rules

Indicate rewrite rules by concatenation. We have two selections so we have two rewrite rules

0 mean what has been selected by the rule 0 + ":" + all the rest of the fields [~] not selected.

--vcf-rewrite-cols "8,9"

Mean on which column must the applied the rules. 8 and 9


In summary This rewrite rule fix the position of the GT column in a vcf files. Example

Column 8            Column 9
...:AF:GT:PQ:...    ...:6.0,8.0:1/1:9.0


SAM tool require that GT is the first field if present. The ruke above will rewrite the VCF to have respecively GT and 1/1 at the beginning of column 8 and 9. The rule "8|\:|[GT]" indicate where GT is positioned in the column 8 while "0|\:|[~]" while the selected field (GT) followed by the other fields

### Score server

score_server --vcf-database "/nvme/IGSB/databases/vcf_database_fix.vcf.bgz"

This enable a score server at port 8888, based on the vcf file vcf_database_fix.vcf.bgz

The server can be querried using a POST method.

curl -X POST 127.0.0.1:8000/query  -H "Content-Type: application/json" --data-binary "{"requests": [{"chromosome": 11, "start": 7899962},{"chromosome": 11, "start": 17895445}]}"

