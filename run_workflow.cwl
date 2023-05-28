cwlVersion: v1.0
class: CommandLineTool

baseCommand: ["python"]

inputs:
  script:
    type: File
  oc_meta:
    type: Directory
  erih_plus:
    type: File
  doaj:
    type: File

arguments:
  - valueFrom: $(inputs.script.path)

outputs:
  result:
    type: stdout
  OCMeta_DOAJ_ErihPlus_merged:
    type: File
    outputBinding:
      glob: "OCMeta_DOAJ_ErihPlus_merged.csv"

stdout: result.txt

