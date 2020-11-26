REGION := eu-west-1

init:
	AWS_PROFILE=$(AWS_PROFILE) terraform init -reconfigure -upgrade

clean:
	#rm -rf .terraform terraform.*

plan:
	@AWS_PROFILE=$(AWS_PROFILE) terraform plan -var 'region=$(REGION)' -var 'account_id=$(shell AWS_PROFILE=$(AWS_PROFILE) aws sts get-caller-identity --query Account --output text)'

apply:
	@AWS_PROFILE=$(AWS_PROFILE) terraform apply -auto-approve -var 'region=$(REGION)' -var 'account_id=$(shell AWS_PROFILE=$(AWS_PROFILE) aws sts get-caller-identity --query Account --output text)'

destroy:
	@AWS_PROFILE=$(AWS_PROFILE) terraform destroy -var 'region=$(REGION)' -var 'account_id=$(shell AWS_PROFILE=$(AWS_PROFILE) aws sts get-caller-identity --query Account --output text)'
