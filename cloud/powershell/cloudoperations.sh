# to connect aws cli
# (for linux) chmod 400 /home/cloudshell-user/labsuser2.pem 
ssh -i C:\Users\patel\Downloads\labsuser2.pem -N -D 8157 hadoop@ec2-54-89-139-219.compute-1.amazonaws.com

# to validate cfn template
aws cloudformation validate-template --template-body file://s3://cf-templates-18314ki7u5xcb-us-east-1/2023305gxg-s3creation.templatergolk8y2w4g

# to move the files to s3, TODO : use python boto3
aws s3 cp "C:\Users\patel\Downloads\artists.csv" s3://music-5bb01e20


hadoop fs -put /final.csv final.csv

aws s3 cp final.csv s3://output-951f64e0/final.csv