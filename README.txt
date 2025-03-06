
# running locally
export FASTLY_KEY="xxxxxxxxxxxxxxxxxxxxxx"
python main.py

# deploying
deployed via: 
`gcloud --project moz-fx-data-billing-prod-9147 run deploy --source . --function main --base-image python312 --region us-west1 --no-allow-unauthenticated`

TODO: move this into terraform!

FASTLY_KEY is set as a secret in the deployment

run via cloud scheduler trigger on the 2nd day of the month
