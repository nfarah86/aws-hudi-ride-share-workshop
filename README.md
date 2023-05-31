

# Installation Guides
### Step 1 Create S3 Buckets

# Step 2 Glone the Repo
```
git clone https://github.com/onehouseinc/aws-hudi-ride-share-workshop.git
cd Infrasture

```

# Step 3 Install Serverless framework
```
npm install -g serverless

serverless config credentials --provider aws --key <ACCESS KEYS>  --secret <SECRET KEYS> -o

npx serverless plugin install -n serverless-dotenv-plugin
npx serverless plugin install -n serverless-python-requirements
npx serverless plugin install -n serverless-glue
q
npx sls deploy OR sls deploy
```