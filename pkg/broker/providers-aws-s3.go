package broker

import (
	"encoding/json"
	"errors"
	"os"
	"strings"
	"time"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/aws/aws-sdk-go/service/s3"
	uuid "github.com/nu7hatch/gouuid"
)

type S3Settings struct {
	Versioned bool   `json:"versioned,omitempty"`
	Encrypted bool   `json:"encrypted,omitempty"`
	KMSKeyId  string `json:"kmsKeyId,omitempty"`
}

type User struct {
	ARN             string
	UserName        string
	AccessKeyId     string
	SecretAccessKey string
}

type UserPolicyStatement struct {
	Resource []string `json:"Resource"`
	Action   []string `json:"Action"`
	Effect   string   `json:"Effect"`
}

type UserPolicy struct {
	Statement []UserPolicyStatement `json:"Statement"`
	Version   string                `json:"Version"`
}

type SimplePolicy struct {
	PolicyName string
	ARN        string
}

type AWSInstanceS3Provider struct {
	Provider
	iam           *iam.IAM
	s3            *s3.S3
	namePrefix    string
	instanceCache map[string]*Instance
}

type Principal struct {
	AWS string `json:"AWS"`
}

type BucketPolicyStatement struct {
	Sid       string    `json:"Sid"`
	Effect    string    `json:"Effect"`
	Principal Principal `json:"Principal"`
	Action    string    `json:"Action"`
	Resource  string    `json:"Resource"`
}

type BucketPolicy struct {
	Version   string                  `json:"Version"`
	ID        string                  `json:"Id"`
	Statement []BucketPolicyStatement `json:"Statement"`
}

func NewAWSInstanceS3Provider(namePrefix string) (*AWSInstanceS3Provider, error) {
	if os.Getenv("AWS_REGION") == "" {
		return nil, errors.New("Unable to find AWS_REGION environment variable.")
	}
	if os.Getenv("AWS_ACCOUNT_ID") == "" {
		return nil, errors.New("Unable to find AWS_ACCOUNT_ID environment variable.")
	}
	t := time.NewTicker(time.Second * 5)
	AWSInstanceS3Provider := &AWSInstanceS3Provider{
		namePrefix:    namePrefix,
		instanceCache: make(map[string]*Instance),
		iam:           iam.New(session.New(&aws.Config{Region: aws.String(os.Getenv("AWS_REGION"))})),
		s3:            s3.New(session.New(&aws.Config{Region: aws.String(os.Getenv("AWS_REGION"))})),
	}
	go (func() {
		for {
			AWSInstanceS3Provider.instanceCache = make(map[string]*Instance)
			<-t.C
		}
	})()
	return AWSInstanceS3Provider, nil
}

func (provider AWSInstanceS3Provider) CreateUser(UserName string) (*User, error) {
	resp, err := provider.iam.CreateUser(&iam.CreateUserInput{
		UserName: aws.String(UserName),
	})
	if err != nil {
		return nil, err
	}
	respkey, err := provider.iam.CreateAccessKey(&iam.CreateAccessKeyInput{
		UserName: aws.String(UserName),
	})
	if err != nil {
		return nil, err
	}
	return &User{
		ARN:             *resp.User.Arn,
		AccessKeyId:     *respkey.AccessKey.AccessKeyId,
		SecretAccessKey: *respkey.AccessKey.SecretAccessKey,
		UserName:        UserName,
	}, nil
}

func (provider AWSInstanceS3Provider) RotateAccessKey(UserName string, ARN string) (*User, error) {
	oldKey, err := provider.GetAccessKeyId(UserName)
	if err != nil {
		return nil, err
	}
	resp, err := provider.iam.CreateAccessKey(&iam.CreateAccessKeyInput{
		UserName: aws.String(UserName),
	})
	if err != nil {
		return nil, err
	}
	_, err = provider.iam.DeleteAccessKey(&iam.DeleteAccessKeyInput{
		AccessKeyId: oldKey,
		UserName:    aws.String(UserName),
	})
	if err != nil {
		return nil, err
	}
	return &User{
		ARN:             ARN,
		AccessKeyId:     *resp.AccessKey.AccessKeyId,
		SecretAccessKey: *resp.AccessKey.SecretAccessKey,
		UserName:        UserName,
	}, nil
}

func (provider AWSInstanceS3Provider) DeleteUser(UserName string) error {
	_, err := provider.iam.DeleteUser(&iam.DeleteUserInput{
		UserName: aws.String(UserName),
	})
	return err
}

func (provider AWSInstanceS3Provider) GetAccessKeyId(BucketName string) (*string, error) {
	res, err := provider.iam.ListAccessKeys(&iam.ListAccessKeysInput{
		UserName: aws.String(BucketName),
	})
	if err != nil {
		return nil, err
	}
	return aws.String(*res.AccessKeyMetadata[0].AccessKeyId), nil
}

func (provider AWSInstanceS3Provider) DeleteAccessKey(BucketName string) error {
	accessKeyId, err := provider.GetAccessKeyId(BucketName)
	if err != nil {
		return err
	}
	_, err = provider.iam.DeleteAccessKey(&iam.DeleteAccessKeyInput{
		AccessKeyId: accessKeyId,
		UserName:    aws.String(BucketName),
	})
	return err
}

func (provider AWSInstanceS3Provider) GetPolicyARN(BucketName string) (*string, error) {
	res, err := provider.iam.ListAttachedUserPolicies(&iam.ListAttachedUserPoliciesInput{
		UserName: aws.String(BucketName),
	})
	if err != nil {
		return nil, err
	}
	return aws.String(*res.AttachedPolicies[0].PolicyArn), nil
}

func (provider AWSInstanceS3Provider) DeleteUserPolicy(policy string) error {
	_, err := provider.iam.DeletePolicy(&iam.DeletePolicyInput{
		PolicyArn: aws.String(policy),
	})
	return err
}

func (provider AWSInstanceS3Provider) DetachUserPolicy(BucketName string) error {
	policy, err := provider.GetPolicyARN(BucketName)
	if err != nil {
		return err
	}

	_, err = provider.iam.DetachUserPolicy(&iam.DetachUserPolicyInput{
		PolicyArn: policy,
		UserName:  aws.String(BucketName),
	})

	if err != nil {
		return err
	}

	return provider.DeleteUserPolicy(*policy)
}

func (provider AWSInstanceS3Provider) CreateUserPolicy(UserName string, BucketName string, Encrypted bool, KMSKeyID string) (*SimplePolicy, error) {
	policy := UserPolicy{
		Version: "2012-10-17",
		Statement: []UserPolicyStatement{
			UserPolicyStatement{
				Effect:   "Allow",
				Resource: []string{"arn:aws:s3:::" + BucketName + "/*", "arn:aws:s3:::" + BucketName},
				Action:   []string{"s3:*"},
			},
		},
	}

	if Encrypted && KMSKeyID != "" {
		policy.Statement = append(policy.Statement, UserPolicyStatement{
			Effect:   "Allow",
			Resource: []string{"arn:aws:kms:" + os.Getenv("AWS_REGION") + ":" + os.Getenv("AWS_ACCOUNT_ID") + ":key/" + KMSKeyID},
			Action:   []string{"kms:Decrypt", "kms:Encrypt", "kms:DescribeKey", "kms:ReEncrypt*", "kms:GenerateDataKey*"},
		})
	}

	policyString, err := json.Marshal(policy)
	if err != nil {
		return nil, err
	}
	res, err := provider.iam.CreatePolicy(&iam.CreatePolicyInput{
		PolicyDocument: aws.String(string(policyString)),
		PolicyName:     aws.String(UserName + "policy"),
	})

	if err != nil {
		return nil, err
	}

	return &SimplePolicy{
		PolicyName: *res.Policy.PolicyName,
		ARN:        *res.Policy.Arn,
	}, nil
}

func (provider AWSInstanceS3Provider) AttachUserPolicy(UserName string, Policy *SimplePolicy) error {
	_, err := provider.iam.AttachUserPolicy(&iam.AttachUserPolicyInput{
		PolicyArn: aws.String(Policy.ARN),
		UserName:  aws.String(UserName),
	})
	return err
}

func (provider AWSInstanceS3Provider) CreateRandomName() string {
	id, _ := uuid.NewV4()
	return provider.namePrefix + "-u" + (strings.Split(id.String(), "-")[0])
}

func (provider AWSInstanceS3Provider) GetInstance(name string, plan *ProviderPlan) (*Instance, error) {
	if provider.instanceCache[name+plan.ID] != nil {
		return provider.instanceCache[name+plan.ID], nil
	}

	ARN, err := provider.GetPolicyARN(name)
	if err != nil {
		return nil, err
	}

	return &Instance{
		Id:            "", // provider should not store this.
		Name:          name,
		ProviderId:    *ARN,
		Plan:          plan,
		Username:      "", // provider should not store this.
		Password:      "", // provider should not store this.
		Endpoint:      "", // provider should not store this.
		Status:        "available",
		Ready:         true,
		Engine:        "s3",
		EngineVersion: "aws-1",
		Scheme:        "s3",
	}, nil
}

func (provider AWSInstanceS3Provider) PerformPostProvision(db *Instance) (*Instance, error) {
	return db, nil
}

func (provider AWSInstanceS3Provider) GetUrl(instance *Instance) map[string]interface{} {
	return map[string]interface{}{
		"S3_BUCKET":     instance.Name,
		"S3_LOCATION":   instance.Endpoint,
		"S3_ACCESS_KEY": instance.Username,
		"S3_SECRET_KEY": instance.Password,
		"S3_REGION":     os.Getenv("AWS_REGION"),
	}
}

func (provider AWSInstanceS3Provider) emptyBucket(BucketName string) error {
	var output *s3.ListObjectsOutput = nil
	var err error = nil
	output, err = provider.s3.ListObjects(&s3.ListObjectsInput{Bucket:aws.String(BucketName)})
	if err != nil {
		return err
	}
	if len(output.Contents) == 0 {
		return nil
	}
	objects := make([]*s3.ObjectIdentifier, 0)
	for _, obj := range output.Contents {
		if(obj != nil && obj.Key != nil) {
			objects = append(objects, &s3.ObjectIdentifier{
				Key:obj.Key,
			})
		}
	}
	_, err = provider.s3.DeleteObjects(&s3.DeleteObjectsInput{
		Bucket:aws.String(BucketName),
		Delete:&s3.Delete{
			Objects:objects,
		},
	})
	if err != nil {
		return err
	}
	if output.IsTruncated != nil && *output.IsTruncated == true  {
		return provider.emptyBucket(BucketName)
	}
	return nil
}


func (provider AWSInstanceS3Provider) emptyBucketVersions(BucketName string) error {
	var output *s3.ListObjectVersionsOutput = nil
	var err error = nil
	output, err = provider.s3.ListObjectVersions(&s3.ListObjectVersionsInput{Bucket:aws.String(BucketName)})
	if err != nil {
		return err
	}
	if len(output.Versions) == 0 && len(output.DeleteMarkers) == 0 {
		return nil
	}
	objects := make([]*s3.ObjectIdentifier, 0)
	for _, obj := range output.Versions {
		if(obj != nil && obj.Key != nil) {
			objects = append(objects, &s3.ObjectIdentifier{
				Key:obj.Key,
				VersionId:obj.VersionId,
			})
		}
	}
	for _, obj := range output.DeleteMarkers {
		if(obj != nil && obj.Key != nil) {
			objects = append(objects, &s3.ObjectIdentifier{
				Key:obj.Key,
				VersionId:obj.VersionId,
			})
		}
	}
	_, err = provider.s3.DeleteObjects(&s3.DeleteObjectsInput{
		Bucket: aws.String(BucketName),
		Delete: &s3.Delete{
			Objects: objects,
		},
	})
	if err != nil {
		return err
	}
	if output.IsTruncated != nil && *output.IsTruncated == true  {
		return provider.emptyBucketVersions(BucketName)
	}
	return nil
}

func (provider AWSInstanceS3Provider) DeleteBucket(BucketName string) error {
	if err := provider.emptyBucket(BucketName); err != nil {
		return err
	}
	if  err := provider.emptyBucketVersions(BucketName); err != nil {
		return err
	}
	_, err := provider.s3.DeleteBucket(&s3.DeleteBucketInput{
		Bucket: aws.String(BucketName),
	})
	return err
}

func (provider AWSInstanceS3Provider) CreateBucket(BucketName string, Plan *S3Settings) (*string, error) {
	res, err := provider.s3.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(BucketName),
	})
	if err != nil {
		return nil, err
	}
	if Plan.Versioned {
		_, err := provider.s3.PutBucketVersioning(&s3.PutBucketVersioningInput{
			Bucket: aws.String(BucketName),
			VersioningConfiguration: &s3.VersioningConfiguration{
				Status: aws.String("Enabled"),
			},
		})
		_, err = provider.s3.PutBucketLifecycleConfiguration(&s3.PutBucketLifecycleConfigurationInput{
			Bucket: aws.String(BucketName),
			LifecycleConfiguration: &s3.BucketLifecycleConfiguration{
				Rules: []*s3.LifecycleRule{
					{
						Prefix: aws.String(""),
						Status: aws.String("Enabled"),
						ID:     aws.String("versioned"),
						NoncurrentVersionExpiration: &s3.NoncurrentVersionExpiration{
							NoncurrentDays: aws.Int64(180),
						},
						NoncurrentVersionTransitions: []*s3.NoncurrentVersionTransition{
							{
								NoncurrentDays: aws.Int64(30),
								StorageClass:   aws.String("STANDARD_IA"),
							},
						},
						Transitions: []*s3.Transition{
							{
								Days:         aws.Int64(30),
								StorageClass: aws.String("STANDARD_IA"),
							},
						},
					},
				},
			},
		})
		if err != nil {
			return nil, err
		}
	}
	if Plan.Encrypted && Plan.KMSKeyId != "" {
		_, err = provider.s3.PutBucketEncryption(&s3.PutBucketEncryptionInput{
			Bucket: aws.String(BucketName),
			ServerSideEncryptionConfiguration: &s3.ServerSideEncryptionConfiguration{
				Rules: []*s3.ServerSideEncryptionRule{
					&s3.ServerSideEncryptionRule{
						ApplyServerSideEncryptionByDefault: &s3.ServerSideEncryptionByDefault{
							KMSMasterKeyID: aws.String(Plan.KMSKeyId),
							SSEAlgorithm:   aws.String("aws:kms"),
						},
					},
				},
			},
		})
		if err != nil {
			return nil, err
		}
	}
	return aws.String(strings.Replace(strings.Replace(*res.Location, "http://", "", -1), "/", "", -1)), nil
}

func (provider AWSInstanceS3Provider) AddBucketPolicy(BucketName string, ARN string) error {
	policy := BucketPolicy{
		Version: "2012-10-17",
		ID:      "Policy47474747",
		Statement: []BucketPolicyStatement{
			BucketPolicyStatement{
				Sid:    "Stmt47474747",
				Effect: "Allow",
				Principal: Principal{
					AWS: ARN,
				},
				Resource: "arn:aws:s3:::" + BucketName + "/*",
				Action:   "s3:*",
			},
		},
	}
	policyString, err := json.Marshal(policy)
	if err != nil {
		return err
	}
	_, err = provider.s3.PutBucketPolicy(&s3.PutBucketPolicyInput{
		Bucket: aws.String(BucketName),
		Policy: aws.String(string(policyString)),
	})
	return err
}

func (provider AWSInstanceS3Provider) GetTags(BucketName string) ([]*s3.Tag, error) {
	res, err := provider.s3.GetBucketTagging(&s3.GetBucketTaggingInput{
		Bucket: aws.String(BucketName),
	})
	if err != nil {
		// If no tags exist it returns a 404.
		return make([]*s3.Tag, 0), nil
	}
	return res.TagSet, nil
}

func (provider AWSInstanceS3Provider) Provision(Id string, plan *ProviderPlan, Owner string) (*Instance, error) {
	var settings S3Settings
	if err := json.Unmarshal([]byte(plan.providerPrivateDetails), &settings); err != nil {
		return nil, err
	}

	name := provider.CreateRandomName()
	user, err := provider.CreateUser(name)
	if err != nil {
		return nil, err
	}

	endpoint, err := provider.CreateBucket(user.UserName, &settings)
	if err != nil {
		return nil, err
	}

	instance := &Instance{
		Id:            Id,
		Name:          name,
		ProviderId:    user.ARN,
		Plan:          plan,
		Username:      user.AccessKeyId,
		Password:      user.SecretAccessKey,
		Endpoint:      *endpoint,
		Status:        "available",
		Ready:         true,
		Engine:        "s3",
		EngineVersion: "aws-1",
		Scheme:        "s3",
	}

	time.Sleep(time.Second * time.Duration(10))
	if err := provider.Tag(instance, "billingcode", Owner); err != nil {
		return nil, err
	}

	if err := provider.AddBucketPolicy(user.UserName, user.ARN); err != nil {
		return nil, err
	}
	policy, err := provider.CreateUserPolicy(user.UserName, user.UserName, settings.Encrypted, settings.KMSKeyId)
	if err != nil {
		return nil, err
	}

	if err := provider.AttachUserPolicy(user.UserName, policy); err != nil {
		return nil, err
	}
	return instance, nil
}

func (provider AWSInstanceS3Provider) Deprovision(Instance *Instance, takeSnapshot bool) error {
	if err := provider.DeleteBucket(Instance.Name); err != nil {
		return err
	}
	if err := provider.DetachUserPolicy(Instance.Name); err != nil {
		return err
	}
	if err := provider.DeleteAccessKey(Instance.Name); err != nil {
		return err
	}
	return provider.DeleteUser(Instance.Name)
}

func (provider AWSInstanceS3Provider) Modify(Instance *Instance, plan *ProviderPlan) (*Instance, error) {
	return nil, errors.New("S3 buckets cannot be modified, only created or destroyed.")
}

func (provider AWSInstanceS3Provider) Tag(Instance *Instance, Name string, Value string) error {
	tags, err := provider.GetTags(Instance.Name)
	if err != nil {
		return err
	}
	_, err = provider.s3.PutBucketTagging(&s3.PutBucketTaggingInput{
		Bucket: aws.String(Instance.Name),
		Tagging: &s3.Tagging{
			TagSet: append(tags, &s3.Tag{Key: aws.String(Name), Value: aws.String(Value)}),
		},
	})
	return err
}

func (provider AWSInstanceS3Provider) Untag(Instance *Instance, Name string) error {
	tags, err := provider.GetTags(Instance.Name)
	if err != nil {
		return err
	}
	var newTags []*s3.Tag = make([]*s3.Tag, 0)
	for _, tag := range tags {
		if *tag.Key != Name {
			newTags = append(newTags, tag)
		}
	}
	_, err = provider.s3.PutBucketTagging(&s3.PutBucketTaggingInput{
		Bucket: aws.String(Instance.Name),
		Tagging: &s3.Tagging{
			TagSet: newTags,
		},
	})
	return err
}

func (provider AWSInstanceS3Provider) RotateCredentials(Instance *Instance) (*User, error) {
	return provider.RotateAccessKey(Instance.Name, Instance.ProviderId)
}
