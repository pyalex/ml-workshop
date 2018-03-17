--runner=DataflowRunner
--input=gs://ml-workshop-vilnus/personal
--output=gs://ml-workshop-vilnus/sample
--tempLocation=gs://ml-workshop-vilnus/temp
--gcpTempLocation=gs://ml-workshop-vilnus/temp
--project=my-project-id


export GOOGLE_APPLICATION_CREDENTIALS=~/.config/gcloud/legacy_credentials/oleksiimo\@wix.com/adc.json

Search for "Dataflow API" on google cloud console and enable it. Then play again.
