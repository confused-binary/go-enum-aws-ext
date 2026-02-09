package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"

	"errors"
	"os/exec"
	"regexp"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/apigateway"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/elasticloadbalancing"
	elbv2 "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	elbv2types "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2/types"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/route53"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/schollz/progressbar/v3"
)

type Resource struct {
	Profile           string
	AccountID         string
	Region            string
	Service           string
	ResourceName      string
	ResourceDetails   string
	ResourceExtra     string
	AdditionalDetails string
}

// Supported services for the -services flag
var supportedServices = []string{
	"EC2", "ElasticIP", "ELB", "ALB", "APIGateway", "Route53", "Lambda", "SQS", "SNS", "RDS", "S3", "IAM",
}

func main() {
	profilesFlag := flag.String("profiles", "default", "AWS profile name or regex pattern")
	noProgress := flag.Bool("no-progress", false, "Suppress progress bar output")
	servicesFlag := flag.String("services", "", "Comma-separated list of services to check. Supported: EC2,ElasticIP,ELB,ALB,APIGateway,Route53,Lambda,SQS,SNS,RDS,S3,IAM")
	delayFlag := flag.Int("delay", 0, "Delay in seconds between requests (default: 0)")
	flag.Parse()

	ctx := context.Background()

	// Determine if profilesFlag is a regex
	profiles := []string{}
	isRegex := false
	regexSpecial := regexp.MustCompile(`[.*+?^${}()|\[\]\\]`)
	if regexSpecial.MatchString(*profilesFlag) {
		isRegex = true
	}

	if isRegex {
		cmd := exec.Command("aws", "configure", "list-profiles")
		out, err := cmd.Output()
		if err != nil {
			printError(nil, "Error listing AWS profiles: %v\n", err)
			os.Exit(1)
		}
		allProfiles := strings.Split(strings.TrimSpace(string(out)), "\n")
		pattern, err := regexp.Compile(*profilesFlag)
		if err != nil {
			printError(nil, "Invalid regex pattern for profiles: %v\n", err)
			os.Exit(1)
		}
		for _, p := range allProfiles {
			if pattern.MatchString(p) {
				profiles = append(profiles, p)
			}
		}
		if len(profiles) == 0 {
			printError(nil, "No profiles matched regex: %s\n", *profilesFlag)
			os.Exit(1)
		}
	} else {
		profiles = append(profiles, *profilesFlag)
	}

	// Parse services flag
	var servicesToCheck map[string]bool
	if *servicesFlag == "" {
		servicesToCheck = make(map[string]bool)
		for _, s := range supportedServices {
			servicesToCheck[s] = true
		}
	} else {
		servicesToCheck = make(map[string]bool)
		for _, s := range strings.Split(*servicesFlag, ",") {
			s = strings.TrimSpace(s)
			for _, sup := range supportedServices {
				if strings.EqualFold(s, sup) {
					servicesToCheck[sup] = true
				}
			}
		}
		if len(servicesToCheck) == 0 {
			printError(nil, "No valid services specified in --services. Supported: %s\n", strings.Join(supportedServices, ","))
			os.Exit(1)
		}
	}

	fmt.Fprintln(os.Stderr) // Blank line before CSV header
	writer := csv.NewWriter(os.Stdout)
	defer writer.Flush()
	writer.Write([]string{"profile", "accountID", "region", "service", "resourceName", "resourceDetails", "additionalDetails", "extraDetails"})
	var globalWg sync.WaitGroup
	var csvMutex sync.Mutex
	var rateMutex sync.Mutex
	delayDuration := time.Duration(*delayFlag) * time.Second

	for _, profile := range profiles {
		globalWg.Add(1)
		go func(profile string) {
			defer globalWg.Done()
			runProfileEnumeration(ctx, profile, *noProgress, servicesToCheck, writer, &csvMutex, &rateMutex, delayDuration)
		}(profile)
	}
	globalWg.Wait()
}

// runProfileEnumeration runs the main logic for a single profile
func runProfileEnumeration(ctx context.Context, profile string, noProgress bool, servicesToCheck map[string]bool, writer *csv.Writer, csvMutex *sync.Mutex, rateMutex *sync.Mutex, delay time.Duration) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithSharedConfigProfile(profile))
	if err != nil {
		printError(nil, "Error loading AWS config for profile %s: %v\n", profile, err)
		return
	}

	stsClient := sts.NewFromConfig(cfg)
	accountID, err := getAccountID(ctx, stsClient)
	if err != nil {
		printError(nil, "Error getting account ID for profile %s: %v\n", profile, err)
		return
	}

	ec2Client := ec2.NewFromConfig(cfg)
	regions, err := getRegions(ctx, ec2Client)
	if err != nil {
		printError(nil, "Error getting regions for profile %s: %v\n", profile, err)
		return
	}

	// Calculate progress bar total
	regionalServices := []string{"EC2", "ElasticIP", "ELB", "ALB", "APIGateway", "Route53", "Lambda", "SQS", "SNS", "RDS"}
	globalServices := []string{"S3", "IAM"}
	numRegional := 0
	numGlobal := 0
	for _, s := range regionalServices {
		if servicesToCheck[s] {
			numRegional++
		}
	}
	for _, s := range globalServices {
		if servicesToCheck[s] {
			numGlobal++
		}
	}

	resourceChan := make(chan Resource, 1000)
	var barMutex sync.Mutex
	var bar *progressbar.ProgressBar
	if !noProgress {
		bar = progressbar.NewOptions(len(regions)*numRegional+numGlobal,
			progressbar.OptionSetWriter(os.Stderr),
			progressbar.OptionShowCount(),
			progressbar.OptionSetTheme(progressbar.Theme{
				Saucer:        "=",
				SaucerHead:    ">",
				SaucerPadding: " ",
				BarStart:      "[",
				BarEnd:        "]",
			}),
			progressbar.OptionSetRenderBlankState(true),
			progressbar.OptionSetDescription("Starting enumeration..."),
		)
	}

	var masterWg sync.WaitGroup
	masterWg.Add(1)
	go func() {
		defer masterWg.Done()
		var wg sync.WaitGroup
		for _, region := range regions {
			wg.Add(1)
			go func(region string) {
				defer wg.Done()
				regionCfg := cfg.Copy()
				regionCfg.Region = region
				enumerateResources(ctx, regionCfg, profile, accountID, region, resourceChan, bar, &barMutex, servicesToCheck, writer, delay, rateMutex)
			}(*region.RegionName)
		}
		wg.Wait()
	}()

	// Add global resources to WaitGroup
	masterWg.Add(1)
	go func() {
		defer masterWg.Done()
		enumerateGlobalResources(ctx, cfg, profile, accountID, resourceChan, bar, &barMutex, servicesToCheck, writer, delay, rateMutex)
	}()

	// Close channel when all enumeration is done
	go func() {
		masterWg.Wait()
		close(resourceChan)
	}()

	// Collect and write resources to CSV
	for resource := range resourceChan {
		details := strings.Split(resource.ResourceDetails, ";")
		for _, detail := range details {
			detail = strings.TrimSpace(detail)
			if detail == "" {
				continue
			}
			csvMutex.Lock()
			writer.Write([]string{
				sanitizeCSVField(resource.Profile),
				sanitizeCSVField(resource.AccountID),
				sanitizeCSVField(resource.Region),
				sanitizeCSVField(resource.Service),
				sanitizeCSVField(resource.ResourceName),
				sanitizeCSVField(detail),
				sanitizeCSVField(resource.AdditionalDetails),
				sanitizeCSVField(resource.ResourceExtra),
			})
			csvMutex.Unlock()
		}
	}
}

func getAccountID(ctx context.Context, client *sts.Client) (string, error) {
	resp, err := client.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return "", err
	}
	return aws.ToString(resp.Account), nil
}

func getRegions(ctx context.Context, client *ec2.Client) ([]ec2types.Region, error) {
	resp, err := client.DescribeRegions(ctx, &ec2.DescribeRegionsInput{})
	if err != nil {
		return nil, err
	}
	return resp.Regions, nil
}

// Helper for throttling/retry logic
func retryOnThrottle(fn func() error) error {
	delays := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second}
	var lastErr error
	for i := 0; i < len(delays)+1; i++ {
		err := fn()
		if err == nil {
			return nil
		}
		var ae interface{ ErrorCode() string }
		if errors.As(err, &ae) {
			code := ae.ErrorCode()
			if code == "Throttling" || code == "ThrottlingException" || code == "RequestLimitExceeded" {
				if i < len(delays) {
					time.Sleep(delays[i])
					continue
				}
			}
		}
		lastErr = err
		break
	}
	return lastErr
}

func enumerateResources(ctx context.Context, cfg aws.Config, profile, accountID, region string, resourceChan chan<- Resource, bar *progressbar.ProgressBar, barMutex *sync.Mutex, servicesToCheck map[string]bool, writer *csv.Writer, delay time.Duration, rateMutex *sync.Mutex) {
	// Create region-specific config
	regionCfg := cfg.Copy()
	regionCfg.Region = region
	ec2Client := ec2.NewFromConfig(regionCfg)

	// Enumerate EC2 Instances
	if servicesToCheck["EC2"] {
		if bar != nil {
			barMutex.Lock()
			bar.Describe(fmt.Sprintf("Enumerating EC2 instances in %s", region))
			barMutex.Unlock()
		}
		enumerateEC2Instances(ctx, ec2Client, profile, accountID, region, resourceChan, writer, delay, rateMutex, bar)
		if bar != nil {
			barMutex.Lock()
			bar.Add(1)
			barMutex.Unlock()
		}
	}

	// Enumerate Elastic IPs
	if servicesToCheck["ElasticIP"] {
		if bar != nil {
			barMutex.Lock()
			bar.Describe(fmt.Sprintf("Enumerating Elastic IPs in %s", region))
			barMutex.Unlock()
		}
		enumerateElasticIPs(ctx, ec2Client, profile, accountID, region, resourceChan, writer, delay, rateMutex, bar)
		if bar != nil {
			barMutex.Lock()
			bar.Add(1)
			barMutex.Unlock()
		}
	}

	// Enumerate Classic Load Balancers
	if servicesToCheck["ELB"] {
		if bar != nil {
			barMutex.Lock()
			bar.Describe(fmt.Sprintf("Enumerating Classic Load Balancers in %s", region))
			barMutex.Unlock()
		}
		elbClient := elasticloadbalancing.NewFromConfig(regionCfg)
		enumerateClassicLoadBalancers(ctx, elbClient, profile, accountID, region, resourceChan, writer, delay, rateMutex, bar)
		if bar != nil {
			barMutex.Lock()
			bar.Add(1)
			barMutex.Unlock()
		}
	}

	// Enumerate Application Load Balancers
	if servicesToCheck["ALB"] {
		if bar != nil {
			barMutex.Lock()
			bar.Describe(fmt.Sprintf("Enumerating Application Load Balancers in %s", region))
			barMutex.Unlock()
		}
		elbv2Client := elbv2.NewFromConfig(regionCfg)
		enumerateApplicationLoadBalancers(ctx, elbv2Client, profile, accountID, region, resourceChan, writer, delay, rateMutex, bar)
		if bar != nil {
			barMutex.Lock()
			bar.Add(1)
			barMutex.Unlock()
		}
	}

	// Enumerate API Gateways
	if servicesToCheck["APIGateway"] {
		if bar != nil {
			barMutex.Lock()
			bar.Describe(fmt.Sprintf("Enumerating API Gateways in %s", region))
			barMutex.Unlock()
		}
		apiGatewayClient := apigateway.NewFromConfig(regionCfg)
		enumerateAPIGateways(ctx, apiGatewayClient, profile, accountID, region, resourceChan, writer, delay, rateMutex, bar)
		if bar != nil {
			barMutex.Lock()
			bar.Add(1)
			barMutex.Unlock()
		}
	}

	// Enumerate Route53 Hosted Zones
	if servicesToCheck["Route53"] {
		if bar != nil {
			barMutex.Lock()
			bar.Describe(fmt.Sprintf("Enumerating Route53 hosted zones in %s", region))
			barMutex.Unlock()
		}
		route53Client := route53.NewFromConfig(regionCfg)
		enumerateRoute53HostedZones(ctx, route53Client, profile, accountID, region, resourceChan, writer, delay, rateMutex, bar)
		if bar != nil {
			barMutex.Lock()
			bar.Add(1)
			barMutex.Unlock()
		}
	}

	// Enumerate Lambda Functions
	if servicesToCheck["Lambda"] {
		if bar != nil {
			barMutex.Lock()
			bar.Describe(fmt.Sprintf("Enumerating Lambda functions in %s", region))
			barMutex.Unlock()
		}
		lambdaClient := lambda.NewFromConfig(regionCfg)
		enumerateLambdaFunctions(ctx, lambdaClient, profile, accountID, region, resourceChan, writer, delay, rateMutex, bar)
		if bar != nil {
			barMutex.Lock()
			bar.Add(1)
			barMutex.Unlock()
		}
	}

	// Enumerate SQS Queues
	if servicesToCheck["SQS"] {
		if bar != nil {
			barMutex.Lock()
			bar.Describe(fmt.Sprintf("Enumerating SQS queues in %s", region))
			barMutex.Unlock()
		}
		sqsClient := sqs.NewFromConfig(regionCfg)
		enumerateSQSQueues(ctx, sqsClient, profile, accountID, region, resourceChan, writer, delay, rateMutex, bar)
		if bar != nil {
			barMutex.Lock()
			bar.Add(1)
			barMutex.Unlock()
		}
	}

	// Enumerate SNS Topics
	if servicesToCheck["SNS"] {
		if bar != nil {
			barMutex.Lock()
			bar.Describe(fmt.Sprintf("Enumerating SNS topics in %s", region))
			barMutex.Unlock()
		}
		snsClient := sns.NewFromConfig(regionCfg)
		enumerateSNSTopics(ctx, snsClient, profile, accountID, region, resourceChan, writer, delay, rateMutex, bar)
		if bar != nil {
			barMutex.Lock()
			bar.Add(1)
			barMutex.Unlock()
		}
	}

	// Enumerate RDS Instances
	if servicesToCheck["RDS"] {
		if bar != nil {
			barMutex.Lock()
			bar.Describe(fmt.Sprintf("Enumerating RDS instances in %s", region))
			barMutex.Unlock()
		}
		rdsClient := rds.NewFromConfig(regionCfg)
		enumerateRDSInstances(ctx, rdsClient, profile, accountID, region, resourceChan, writer, delay, rateMutex, bar)
		if bar != nil {
			barMutex.Lock()
			bar.Add(1)
			barMutex.Unlock()
		}
	}
}

// EC2 Instances
func enumerateEC2Instances(ctx context.Context, client *ec2.Client, profile, accountID, region string, resourceChan chan<- Resource, writer *csv.Writer, delay time.Duration, rateMutex *sync.Mutex, bar *progressbar.ProgressBar) {
	if delay > 0 {
		rateMutex.Lock()
		time.Sleep(delay)
		rateMutex.Unlock()
	}
	var resp *ec2.DescribeInstancesOutput
	err := retryOnThrottle(func() error {
		var err error
		resp, err = client.DescribeInstances(ctx, &ec2.DescribeInstancesInput{})
		return err
	})
	if err != nil {
		printError(bar, "Error enumerating EC2 instances in %s: %v\n", region, err)
		return
	}
	for _, reservation := range resp.Reservations {
		for _, instance := range reservation.Instances {
			eip := ""
			if instance.PublicIpAddress != nil {
				eip = aws.ToString(instance.PublicIpAddress)
			}
			resourceChan <- Resource{
				Profile:         profile,
				AccountID:       accountID,
				Region:          region,
				Service:         "EC2",
				ResourceName:    aws.ToString(instance.InstanceId),
				ResourceDetails: eip,
			}
		}
	}
}

// Elastic IPs
func enumerateElasticIPs(ctx context.Context, client *ec2.Client, profile, accountID, region string, resourceChan chan<- Resource, writer *csv.Writer, delay time.Duration, rateMutex *sync.Mutex, bar *progressbar.ProgressBar) {
	if delay > 0 {
		rateMutex.Lock()
		time.Sleep(delay)
		rateMutex.Unlock()
	}
	var resp *ec2.DescribeAddressesOutput
	err := retryOnThrottle(func() error {
		var err error
		resp, err = client.DescribeAddresses(ctx, &ec2.DescribeAddressesInput{})
		return err
	})
	if err != nil {
		printError(bar, "Error enumerating Elastic IPs in %s: %v\n", region, err)
		return
	}
	for _, address := range resp.Addresses {
		resourceChan <- Resource{
			Profile:         profile,
			AccountID:       accountID,
			Region:          region,
			Service:         "ElasticIP",
			ResourceName:    aws.ToString(address.AllocationId),
			ResourceDetails: aws.ToString(address.PublicIp),
		}
	}
}

// Classic Load Balancers
func enumerateClassicLoadBalancers(ctx context.Context, client *elasticloadbalancing.Client, profile, accountID, region string, resourceChan chan<- Resource, writer *csv.Writer, delay time.Duration, rateMutex *sync.Mutex, bar *progressbar.ProgressBar) {
	if delay > 0 {
		rateMutex.Lock()
		time.Sleep(delay)
		rateMutex.Unlock()
	}
	var resp *elasticloadbalancing.DescribeLoadBalancersOutput
	err := retryOnThrottle(func() error {
		var err error
		resp, err = client.DescribeLoadBalancers(ctx, &elasticloadbalancing.DescribeLoadBalancersInput{})
		return err
	})
	if err != nil {
		printError(bar, "Error enumerating Classic Load Balancers in %s: %v\n", region, err)
		return
	}
	for _, lb := range resp.LoadBalancerDescriptions {
		resourceChan <- Resource{
			Profile:         profile,
			AccountID:       accountID,
			Region:          region,
			Service:         "ELB",
			ResourceName:    aws.ToString(lb.LoadBalancerName),
			ResourceDetails: aws.ToString(lb.DNSName),
		}
	}
}

// Application Load Balancers
func enumerateApplicationLoadBalancers(ctx context.Context, client *elbv2.Client, profile, accountID, region string, resourceChan chan<- Resource, writer *csv.Writer, delay time.Duration, rateMutex *sync.Mutex, bar *progressbar.ProgressBar) {
	if delay > 0 {
		rateMutex.Lock()
		time.Sleep(delay)
		rateMutex.Unlock()
	}
	var resp *elbv2.DescribeLoadBalancersOutput
	err := retryOnThrottle(func() error {
		var err error
		resp, err = client.DescribeLoadBalancers(ctx, &elbv2.DescribeLoadBalancersInput{})
		return err
	})
	if err != nil {
		printError(bar, "Error enumerating Application Load Balancers in %s: %v\n", region, err)
		return
	}
	for _, lb := range resp.LoadBalancers {
		if lb.Type == elbv2types.LoadBalancerTypeEnumApplication {
			listenerResp, err := client.DescribeListeners(ctx, &elbv2.DescribeListenersInput{LoadBalancerArn: lb.LoadBalancerArn})
			if err != nil {
				printError(bar, "Error getting listeners for ALB %s in %s: %v\n", aws.ToString(lb.LoadBalancerName), region, err)
				continue
			}
			var rules []string
			for _, listener := range listenerResp.Listeners {
				ruleResp, err := client.DescribeRules(ctx, &elbv2.DescribeRulesInput{ListenerArn: listener.ListenerArn})
				if err != nil {
					printError(bar, "Error getting rules for listener %s in %s: %v\n", aws.ToString(listener.ListenerArn), region, err)
					continue
				}
				for _, rule := range ruleResp.Rules {
					rules = append(rules, fmt.Sprintf("Rule: Priority=%s, Actions=%v, Conditions=%v", aws.ToString(rule.Priority), rule.Actions, rule.Conditions))
				}
			}
			details := fmt.Sprintf("DNS=%s, Rules=[%s]", aws.ToString(lb.DNSName), strings.Join(rules, "; "))
			resourceChan <- Resource{
				Profile:         profile,
				AccountID:       accountID,
				Region:          region,
				Service:         "ALB",
				ResourceName:    aws.ToString(lb.LoadBalancerName),
				ResourceDetails: details,
			}
		}
	}
}

// API Gateways
func enumerateAPIGateways(ctx context.Context, client *apigateway.Client, profile, accountID, region string, resourceChan chan<- Resource, writer *csv.Writer, delay time.Duration, rateMutex *sync.Mutex, bar *progressbar.ProgressBar) {
	if delay > 0 {
		rateMutex.Lock()
		time.Sleep(delay)
		rateMutex.Unlock()
	}
	var resp *apigateway.GetRestApisOutput
	err := retryOnThrottle(func() error {
		var err error
		resp, err = client.GetRestApis(ctx, &apigateway.GetRestApisInput{})
		return err
	})
	if err != nil {
		printError(bar, "Error enumerating API Gateways in %s: %v\n", region, err)
		return
	}
	for _, api := range resp.Items {
		resResp, err := client.GetResources(ctx, &apigateway.GetResourcesInput{RestApiId: api.Id})
		if err != nil {
			printError(bar, "Error getting resources for API %s in %s: %v\n", aws.ToString(api.Name), region, err)
			continue
		}
		apiDNS := fmt.Sprintf("%s.%s.apigateway.%s.amazonaws.com", aws.ToString(api.Id), region, region)
		for _, res := range resResp.Items {
			if res.Path != nil {
				for method := range res.ResourceMethods {
					pathMethod := fmt.Sprintf("%s:%s", *res.Path, method)
					resourceChan <- Resource{
						Profile:           profile,
						AccountID:         accountID,
						Region:            region,
						Service:           "APIGateway",
						ResourceName:      aws.ToString(api.Name),
						ResourceDetails:   apiDNS,
						AdditionalDetails: strings.TrimSpace(pathMethod),
						ResourceExtra:     "PATH",
					}
				}
			}
		}
	}
}

// Route53 Hosted Zones
func enumerateRoute53HostedZones(ctx context.Context, client *route53.Client, profile, accountID, region string, resourceChan chan<- Resource, writer *csv.Writer, delay time.Duration, rateMutex *sync.Mutex, bar *progressbar.ProgressBar) {
	if delay > 0 {
		rateMutex.Lock()
		time.Sleep(delay)
		rateMutex.Unlock()
	}
	var resp *route53.ListHostedZonesOutput
	err := retryOnThrottle(func() error {
		var err error
		resp, err = client.ListHostedZones(ctx, &route53.ListHostedZonesInput{})
		return err
	})
	if err != nil {
		printError(bar, "Error enumerating Route53 hosted zones: %v\n", err)
		return
	}
	for _, zone := range resp.HostedZones {
		if !zone.Config.PrivateZone {
			rrResp, err := client.ListResourceRecordSets(ctx, &route53.ListResourceRecordSetsInput{HostedZoneId: zone.Id})
			if err != nil {
				printError(bar, "Error getting record sets for zone %s: %v\n", aws.ToString(zone.Name), err)
				continue
			}
			for _, rr := range rrResp.ResourceRecordSets {
				if rr.ResourceRecords != nil {
					for _, rec := range rr.ResourceRecords {
						resourceChan <- Resource{
							Profile:         profile,
							AccountID:       accountID,
							Region:          region,
							Service:         "Route53",
							ResourceName:    strings.TrimSuffix(aws.ToString(rr.Name), "."),
							ResourceDetails: aws.ToString(rec.Value),
							ResourceExtra:   string(rr.Type),
						}
					}
				}
			}
		}
	}
}

// Lambda Functions
func enumerateLambdaFunctions(ctx context.Context, client *lambda.Client, profile, accountID, region string, resourceChan chan<- Resource, writer *csv.Writer, delay time.Duration, rateMutex *sync.Mutex, bar *progressbar.ProgressBar) {
	if delay > 0 {
		rateMutex.Lock()
		time.Sleep(delay)
		rateMutex.Unlock()
	}
	var resp *lambda.ListFunctionsOutput
	err := retryOnThrottle(func() error {
		var err error
		resp, err = client.ListFunctions(ctx, &lambda.ListFunctionsInput{})
		return err
	})
	if err != nil {
		printError(bar, "Error enumerating Lambda functions in %s: %v\n", region, err)
		return
	}
	for _, fn := range resp.Functions {
		urlResp, err := client.GetFunctionUrlConfig(ctx, &lambda.GetFunctionUrlConfigInput{FunctionName: fn.FunctionName})
		if err == nil && urlResp.FunctionUrl != nil {
			resourceChan <- Resource{
				Profile:         profile,
				AccountID:       accountID,
				Region:          region,
				Service:         "Lambda",
				ResourceName:    aws.ToString(fn.FunctionName),
				ResourceDetails: aws.ToString(urlResp.FunctionUrl),
				ResourceExtra:   string(fn.Runtime),
			}
		}
		resourceChan <- Resource{
			Profile:         profile,
			AccountID:       accountID,
			Region:          region,
			Service:         "Lambda",
			ResourceName:    aws.ToString(fn.FunctionName),
			ResourceDetails: aws.ToString(fn.FunctionArn),
			ResourceExtra:   string(fn.Runtime),
		}
	}
}

// SQS Queues
func enumerateSQSQueues(ctx context.Context, client *sqs.Client, profile, accountID, region string, resourceChan chan<- Resource, writer *csv.Writer, delay time.Duration, rateMutex *sync.Mutex, bar *progressbar.ProgressBar) {
	if delay > 0 {
		rateMutex.Lock()
		time.Sleep(delay)
		rateMutex.Unlock()
	}
	var resp *sqs.ListQueuesOutput
	err := retryOnThrottle(func() error {
		var err error
		resp, err = client.ListQueues(ctx, &sqs.ListQueuesInput{})
		return err
	})
	if err != nil {
		printError(bar, "Error enumerating SQS queues in %s: %v\n", region, err)
		return
	}
	for _, queueURL := range resp.QueueUrls {
		attrResp, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
			QueueUrl:       aws.String(queueURL),
			AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
		})
		if err != nil {
			printError(bar, "Error getting SQS queue attributes for %s in %s: %v\n", queueURL, region, err)
			continue
		}
		queueName := queueURL[strings.LastIndex(queueURL, "/")+1:]
		arn := attrResp.Attributes[string(sqstypes.QueueAttributeNameQueueArn)]
		resourceChan <- Resource{
			Profile:         profile,
			AccountID:       accountID,
			Region:          region,
			Service:         "SQS",
			ResourceName:    queueName,
			ResourceDetails: queueURL,
			ResourceExtra:   "Standard",
		}
		resourceChan <- Resource{
			Profile:         profile,
			AccountID:       accountID,
			Region:          region,
			Service:         "SQS",
			ResourceName:    queueName,
			ResourceDetails: arn,
			ResourceExtra:   "Standard",
		}
	}
}

// SNS Topics
func enumerateSNSTopics(ctx context.Context, client *sns.Client, profile, accountID, region string, resourceChan chan<- Resource, writer *csv.Writer, delay time.Duration, rateMutex *sync.Mutex, bar *progressbar.ProgressBar) {
	if delay > 0 {
		rateMutex.Lock()
		time.Sleep(delay)
		rateMutex.Unlock()
	}
	var resp *sns.ListTopicsOutput
	err := retryOnThrottle(func() error {
		var err error
		resp, err = client.ListTopics(ctx, &sns.ListTopicsInput{})
		return err
	})
	if err != nil {
		printError(bar, "Error enumerating SNS topics in %s: %v\n", region, err)
		return
	}
	for _, topic := range resp.Topics {
		topicName := aws.ToString(topic.TopicArn)[strings.LastIndex(aws.ToString(topic.TopicArn), ":")+1:]
		endpoint := fmt.Sprintf("sns.%s.amazonaws.com", region)
		resourceChan <- Resource{
			Profile:         profile,
			AccountID:       accountID,
			Region:          region,
			Service:         "SNS",
			ResourceName:    topicName,
			ResourceDetails: endpoint,
		}
		resourceChan <- Resource{
			Profile:         profile,
			AccountID:       accountID,
			Region:          region,
			Service:         "SNS",
			ResourceName:    topicName,
			ResourceDetails: aws.ToString(topic.TopicArn),
		}
	}
}

// RDS Instances
func enumerateRDSInstances(ctx context.Context, client *rds.Client, profile, accountID, region string, resourceChan chan<- Resource, writer *csv.Writer, delay time.Duration, rateMutex *sync.Mutex, bar *progressbar.ProgressBar) {
	if delay > 0 {
		rateMutex.Lock()
		time.Sleep(delay)
		rateMutex.Unlock()
	}
	var resp *rds.DescribeDBInstancesOutput
	err := retryOnThrottle(func() error {
		var err error
		resp, err = client.DescribeDBInstances(ctx, &rds.DescribeDBInstancesInput{})
		return err
	})
	if err != nil {
		printError(bar, "Error enumerating RDS instances in %s: %v\n", region, err)
		return
	}
	for _, db := range resp.DBInstances {
		dns := ""
		if db.Endpoint != nil {
			dns = aws.ToString(db.Endpoint.Address)
		}
		resourceChan <- Resource{
			Profile:         profile,
			AccountID:       accountID,
			Region:          region,
			Service:         "RDS",
			ResourceName:    aws.ToString(db.DBInstanceIdentifier),
			ResourceDetails: dns,
			ResourceExtra:   aws.ToString(db.Engine),
		}
	}
}

// S3 Buckets (Global)

// S3 Buckets (Global)
func enumerateS3Buckets(ctx context.Context, cfg aws.Config, profile, accountID string, resourceChan chan<- Resource, bar *progressbar.ProgressBar, barMutex *sync.Mutex, writer *csv.Writer, delay time.Duration, rateMutex *sync.Mutex) {
	if bar != nil {
		barMutex.Lock()
		bar.Describe("Enumerating S3 buckets (global)")
		barMutex.Unlock()
	}
	client := s3.NewFromConfig(cfg)
	if delay > 0 {
		rateMutex.Lock()
		time.Sleep(delay)
		rateMutex.Unlock()
	}
	var resp *s3.ListBucketsOutput
	err := retryOnThrottle(func() error {
		var err error
		resp, err = client.ListBuckets(ctx, &s3.ListBucketsInput{})
		return err
	})
	if err != nil {
		printError(bar, "Error enumerating S3 buckets: %v\n", err)
		if bar != nil {
			barMutex.Lock()
			bar.Add(1)
			barMutex.Unlock()
		}
		return
	}
	if resp == nil || resp.Buckets == nil {
		printError(bar, "No S3 buckets found or response is nil\n")
		if bar != nil {
			barMutex.Lock()
			bar.Add(1)
			barMutex.Unlock()
		}
		return
	}
	for _, bucket := range resp.Buckets {
		if bucket.Name != nil {
			resourceChan <- Resource{
				Profile:         profile,
				AccountID:       accountID,
				Region:          "global",
				Service:         "S3",
				ResourceName:    aws.ToString(bucket.Name),
				ResourceDetails: fmt.Sprintf("s3://%s", aws.ToString(bucket.Name)),
			}
		}
	}
	if bar != nil {
		barMutex.Lock()
		bar.Add(1)
		barMutex.Unlock()
	}
}

// IAM Users (Global)

// IAM Users (Global)
func enumerateIAMUsers(ctx context.Context, cfg aws.Config, profile, accountID string, resourceChan chan<- Resource, bar *progressbar.ProgressBar, barMutex *sync.Mutex, writer *csv.Writer, delay time.Duration, rateMutex *sync.Mutex) {
	if bar != nil {
		barMutex.Lock()
		bar.Describe("Enumerating IAM users (global)")
		barMutex.Unlock()
	}
	client := iam.NewFromConfig(cfg)
	if delay > 0 {
		rateMutex.Lock()
		time.Sleep(delay)
		rateMutex.Unlock()
	}
	var resp *iam.ListUsersOutput
	err := retryOnThrottle(func() error {
		var err error
		resp, err = client.ListUsers(ctx, &iam.ListUsersInput{})
		return err
	})
	if err != nil {
		printError(bar, "Error enumerating IAM users: %v\n", err)
		if bar != nil {
			barMutex.Lock()
			bar.Add(1)
			barMutex.Unlock()
		}
		return
	}
	if resp == nil || resp.Users == nil {
		printError(bar, "No IAM users found or response is nil\n")
		if bar != nil {
			barMutex.Lock()
			bar.Add(1)
			barMutex.Unlock()
		}
		return
	}
	for _, user := range resp.Users {
		if user.UserName != nil {
			resourceChan <- Resource{
				Profile:         profile,
				AccountID:       accountID,
				Region:          "global",
				Service:         "IAM",
				ResourceName:    aws.ToString(user.UserName),
				ResourceDetails: aws.ToString(user.Arn),
			}
		}
	}
	if bar != nil {
		barMutex.Lock()
		bar.Add(1)
		barMutex.Unlock()
	}
}

// Global Resources (S3, IAM)
func enumerateGlobalResources(ctx context.Context, cfg aws.Config, profile, accountID string, resourceChan chan<- Resource, bar *progressbar.ProgressBar, barMutex *sync.Mutex, servicesToCheck map[string]bool, writer *csv.Writer, delay time.Duration, rateMutex *sync.Mutex) {
	var wg sync.WaitGroup
	if servicesToCheck["S3"] {
		wg.Add(1)
		go func() {
			defer wg.Done()
			enumerateS3Buckets(ctx, cfg, profile, accountID, resourceChan, bar, barMutex, writer, delay, rateMutex)
		}()
	}
	if servicesToCheck["IAM"] {
		wg.Add(1)
		go func() {
			defer wg.Done()
			enumerateIAMUsers(ctx, cfg, profile, accountID, resourceChan, bar, barMutex, writer, delay, rateMutex)
		}()
	}
	wg.Wait()
}

// Helper to sanitize CSV fields (remove all quotation marks and trim spaces)
func sanitizeCSVField(s string) string {
	return strings.TrimSpace(strings.ReplaceAll(s, "\"", ""))
}

// Helper to print errors in red
func printError(bar *progressbar.ProgressBar, format string, a ...interface{}) {
	if bar != nil {
		bar.Clear()
	}
	fmt.Fprintf(os.Stderr, "\033[31m"+format+"\033[0m", a...)
	if bar != nil {
		bar.RenderBlank()
	}
}
