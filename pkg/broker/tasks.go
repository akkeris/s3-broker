package broker

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"github.com/golang/glog"
	"net/http"
	"os"
	"time"
)

type TaskAction string

const (
	DeleteTask                           TaskAction = "delete"
	ResyncFromProviderTask               TaskAction = "resync-from-provider"
	ResyncFromProviderUntilAvailableTask TaskAction = "resync-until-available"
	NotifyCreateServiceWebhookTask       TaskAction = "notify-create-service-webhook"
	NotifyCreateBindingWebhookTask       TaskAction = "notify-create-binding-webhook"
	ChangeProvidersTask					 TaskAction = "change-providers"
	ChangePlansTask						 TaskAction = "change-plans"
	RestoreDbTask						 TaskAction = "restore-database"
	PerformPostProvisionTask			 TaskAction = "perform-post-provision"
)

type Task struct {
	Id         string
	Action     TaskAction
	ResourceId string
	Status     string
	Retries    int64
	Metadata   string
	Result     string
	Started    *time.Time
	Finished   *time.Time
}

type WebhookTaskMetadata struct {
	Url    string `json:"url"`
	Secret string `json:"secret"`
}

type ChangeProvidersTaskMetadata struct {
	Plan string `json:"plan"`
}

type ChangePlansTaskMetadata struct {
	Plan string `json:"plan"`
}

type RestoreDbTaskMetadata struct {
	Backup string `json:"backup"`
}

func FinishedTask(storage Storage, taskId string, retries int64, result string, status string) {
	var t = time.Now()
	err := storage.UpdateTask(taskId, &status, &retries, nil, &result, nil, &t)
	if err != nil {
		glog.Errorf("Unable to update task %s due to: %s (taskId: %s, retries: %d, result: [%s], status: [%s]\n", taskId, err.Error(), taskId, retries, result, status)
	}
}

func UpdateTaskStatus(storage Storage, taskId string, retries int64, result string, status string) {
	err := storage.UpdateTask(taskId, &status, &retries, nil, &result, nil, nil)
	if err != nil {
		glog.Errorf("Unable to update task %s due to: %s (taskId: %s, retries: %d, result: [%s], status: [%s]\n", taskId, err.Error(), taskId, retries, result, status)
	}
}

func RunPreprovisionTasks(ctx context.Context, o Options, namePrefix string, storage Storage, wait int64) {
	t := time.NewTicker(time.Second * time.Duration(wait))
	dbEntries, err := storage.StartProvisioningTasks()
	if err != nil {
		glog.Errorf("Get pending tasks failed: %s\n", err.Error())
		return
	}
	for _, entry := range dbEntries {
		glog.Infof("Starting preprovisioning database: %s with plan: %s\n", entry.Id, entry.PlanId)

		plan, err := storage.GetPlanByID(entry.PlanId)
		if err != nil {
			glog.Errorf("Unable to provision, cannot find plan: %s, %s\n", entry.PlanId, err.Error())
			storage.NukeInstance(entry.Id)
			continue
		}
		provider, err := GetProviderByPlan(namePrefix, plan)
		if err != nil {
			glog.Errorf("Unable to provision, cannot find provider (GetProviderByPlan failed): %s\n", err.Error())
			storage.NukeInstance(entry.Id)
			continue
		}

		Instance, err := provider.Provision(entry.Id, plan, "preprovisioned")
		if err != nil {
			glog.Errorf("Error provisioning database (%s): %s\n", plan.ID, err.Error())
			storage.NukeInstance(entry.Id)
			continue
		}

		if err = storage.UpdateInstance(Instance, Instance.Plan.ID); err != nil {
			glog.Errorf("Error inserting record into provisioned table: %s\n", err.Error())

			if err = provider.Deprovision(Instance, false); err != nil {
				glog.Errorf("Error cleaning up (deprovision failed) after insert record failed but provision succeeded (Database Id:%s Name: %s) %s\n", Instance.Id, Instance.Name, err.Error())
				if _, err = storage.AddTask(Instance.Id, DeleteTask, Instance.Name); err != nil {
					glog.Errorf("Error: Unable to add task to delete instance, WE HAVE AN ORPHAN! (%s): %s\n", Instance.Name, err.Error())
				}
			}
			continue
		}
		if !IsAvailable(Instance.Status) {
			if _, err = storage.AddTask(Instance.Id, ResyncFromProviderUntilAvailableTask, ""); err != nil {
				glog.Errorf("Error: Unable to schedule resync from provider! (%s): %s\n", Instance.Name, err.Error())
			}
		}
		glog.Infof("Finished preprovisioning database: %s with plan: %s\n", entry.Id, entry.PlanId)
		<-t.C
	}
}

func TickTocPreprovisionTasks(ctx context.Context, o Options, namePrefix string, storage Storage) {
	next_check := time.NewTicker(time.Second * 60 * 5)
	for {
		RunPreprovisionTasks(ctx, o, namePrefix, storage, 60)
		<-next_check.C
	}
}

func UpgradeWithinProviders(storage Storage, fromDb *Instance, toPlanId string, namePrefix string) (string, error) {
	toPlan, err := storage.GetPlanByID(toPlanId)
	if err != nil {
		return "", err
	}
	fromProvider, err := GetProviderByPlan(namePrefix, fromDb.Plan)
	if err != nil {
		return "", err
	}
	if toPlanId == fromDb.Plan.ID {
		return "", errors.New("Cannot upgrade to the same plan")
	}
	if toPlan.Provider != fromDb.Plan.Provider {
		return "", errors.New("Unable to upgrade, different providers were passed in on both plans")
	}

	// This could take a very long time.
	Instance, err := fromProvider.Modify(fromDb, toPlan)
	if err != nil && err.Error() == "This feature is not available on this plan." {
		return UpgradeAcrossProviders(storage, fromDb, toPlanId, namePrefix)
	}
	if err != nil {
		return "", err
	}

	if err = storage.UpdateInstance(Instance, Instance.Plan.ID); err != nil {
		glog.Errorf("ERROR: Cannot update instance in database after upgrade change %s (to plan: %s) %s\n", Instance.Name, Instance.Plan.ID, err.Error())
		return "", err
	}

	if !IsAvailable(Instance.Status) {
		if _, err = storage.AddTask(Instance.Id, ResyncFromProviderTask, ""); err != nil {
			glog.Errorf("Error: Unable to schedule resync from provider! (%s): %s\n", Instance.Name, err.Error())
		}
	}
	return "", err
}

func UpgradeAcrossProviders(storage Storage, fromDb *Instance, toPlanId string, namePrefix string) (string, error) {
	return "", errors.New("Memcached and redis instances cannot be upgraded across providers.")
}

func RunWorkerTasks(ctx context.Context, o Options, namePrefix string, storage Storage) error {

	t := time.NewTicker(time.Second * 60)
	for {
		<-t.C
		storage.WarnOnUnfinishedTasks()

		task, err := storage.PopPendingTask()
		if err != nil && err.Error() != "sql: no rows in result set" {
			glog.Errorf("Getting a pending task failed: %s\n", err.Error())
			return err
		} else if err != nil && err.Error() == "sql: no rows in result set" {
			// Nothing to do...
			continue
		}

		glog.Infof("Started task: %s\n", task.Id)

		if task.Action == DeleteTask {
			glog.Infof("Delete and deprovision database for task: %s\n", task.Id)

			if task.Retries >= 10 {
				glog.Infof("Retry limit was reached for task: %s %d\n", task.Id, task.Retries)
				FinishedTask(storage, task.Id, task.Retries, "Unable to delete database "+task.ResourceId+" as it failed multiple times ("+task.Result+")", "failed")
				continue
			}

			Instance, err := GetInstanceById(namePrefix, storage, task.ResourceId)

			if err != nil {
				UpdateTaskStatus(storage, task.Id, task.Retries+1, "Cannot get Instance: "+err.Error(), "pending")
				continue
			}
			provider, err := GetProviderByPlan(namePrefix, Instance.Plan)
			if err != nil {
				UpdateTaskStatus(storage, task.Id, task.Retries+1, "Cannot get provider: "+err.Error(), "pending")
				continue
			}
			if err = provider.Deprovision(Instance, true); err != nil {
				UpdateTaskStatus(storage, task.Id, task.Retries+1, "Failed to deprovision: "+err.Error(), "pending")
				continue
			}
			if err = storage.DeleteInstance(Instance); err != nil {
				UpdateTaskStatus(storage, task.Id, task.Retries+1, "Failed to delete: "+err.Error(), "pending")
				continue
			}
			FinishedTask(storage, task.Id, task.Retries, "", "finished")
		} else if task.Action == ResyncFromProviderTask {
			glog.Infof("Resyncing from provider for task: %s\n", task.Id)
			if task.Retries >= 60 {
				glog.Infof("Retry limit was reached for task: %s %d\n", task.Id, task.Retries)
				FinishedTask(storage, task.Id, task.Retries, "Unable to resync information from provider for database "+task.ResourceId+" as it failed multiple times ("+task.Result+")", "failed")
				continue
			}
			Instance, err := GetInstanceById(namePrefix, storage, task.ResourceId)
			if err != nil {
				glog.Infof("Failed to get provider instance for task: %s, %s\n", task.Id, err.Error())
				UpdateTaskStatus(storage, task.Id, task.Retries+1, "Cannot get Instance: "+err.Error(), "pending")
				continue
			}
			Entry, err := storage.GetInstance(task.ResourceId)
			if err != nil {
				glog.Infof("Failed to get database instance for task: %s, %s\n", task.Id, err.Error())
				UpdateTaskStatus(storage, task.Id, task.Retries+1, "Cannot get Entry: "+err.Error(), "pending")
				continue
			}
			if Instance.Status != Entry.Status {
				if err = storage.UpdateInstance(Instance, Instance.Plan.ID); err != nil {
					UpdateTaskStatus(storage, task.Id, task.Retries+1, "Failed to update instance: "+err.Error(), "pending")
					continue
				}
			} else {
				glog.Infof("Status did not change at provider for task: %s\n", task.Id)
				UpdateTaskStatus(storage, task.Id, task.Retries+1, "No change in status since last check", "pending")
				continue
			}

			FinishedTask(storage, task.Id, task.Retries, "", "finished")
		} else if task.Action == ResyncFromProviderUntilAvailableTask {
			glog.Infof("Resyncing from provider until available for task: %s\n", task.Id)
			if task.Retries >= 60 {
				glog.Infof("Retry limit was reached for task: %s %d\n", task.Id, task.Retries)
				FinishedTask(storage, task.Id, task.Retries, "Unable to resync information from provider for database "+task.ResourceId+" as it failed multiple times ("+task.Result+")", "failed")
				continue
			}
			Instance, err := GetInstanceById(namePrefix, storage, task.ResourceId)
			if err != nil {
				glog.Infof("Failed to get provider instance for task: %s, %s\n", task.Id, err.Error())
				UpdateTaskStatus(storage, task.Id, task.Retries+1, "Cannot get Instance: "+err.Error(), "pending")
				continue
			}
			if err = storage.UpdateInstance(Instance, Instance.Plan.ID); err != nil {
				UpdateTaskStatus(storage, task.Id, task.Retries+1, "Failed to update instance: "+err.Error(), "pending")
				continue
			}
			if !IsAvailable(Instance.Status) {
				glog.Infof("Status did not change at provider for task: %s\n", task.Id)
				UpdateTaskStatus(storage, task.Id, task.Retries+1, "No change in status since last check (" + Instance.Status + ")", "pending")
				continue
			}
			FinishedTask(storage, task.Id, task.Retries, "", "finished")
		} else if task.Action == PerformPostProvisionTask {
			glog.Infof("Resyncing from provider until available (for perform post provision) for task: %s\n", task.Id)
			if task.Retries >= 60 {
				glog.Infof("Retry limit was reached for task: %s %d\n", task.Id, task.Retries)
				FinishedTask(storage, task.Id, task.Retries, "Unable to resync information from provider for database "+task.ResourceId+" as it failed multiple times ("+task.Result+")", "failed")
				continue
			}
			Instance, err := GetInstanceById(namePrefix, storage, task.ResourceId)
			if err != nil {
				glog.Infof("Failed to get provider instance for task: %s, %s\n", task.Id, err.Error())
				UpdateTaskStatus(storage, task.Id, task.Retries, "Cannot get Instance: "+err.Error(), "pending")
				continue
			}
			if err = storage.UpdateInstance(Instance, Instance.Plan.ID); err != nil {
				UpdateTaskStatus(storage, task.Id, task.Retries+1, "Failed to update instance: "+err.Error(), "pending")
				continue
			}
			if !IsAvailable(Instance.Status) {
				glog.Infof("Status did not change at provider for task: %s\n", task.Id)
				UpdateTaskStatus(storage, task.Id, task.Retries+1, "No change in status since last check (" + Instance.Status + ")", "pending")
				continue
			}

			provider, err := GetProviderByPlan(namePrefix, Instance.Plan)
			if err != nil {
				UpdateTaskStatus(storage, task.Id, task.Retries, "Cannot get provider: " + err.Error(), "pending")
				continue
			}

			newInstance, err := provider.PerformPostProvision(Instance)
			if err != nil {
				UpdateTaskStatus(storage, task.Id, task.Retries+1, "Failed to update instance: " + err.Error(), "pending")
				continue
			}

			if err = storage.UpdateInstance(newInstance, newInstance.Plan.ID); err != nil {
				UpdateTaskStatus(storage, task.Id, task.Retries+1, "Failed to update instance after post provision: "+err.Error(), "pending")
				continue
			}

			FinishedTask(storage, task.Id, task.Retries, "", "finished")
		} else if task.Action == NotifyCreateServiceWebhookTask {

			if task.Retries >= 60 {
				FinishedTask(storage, task.Id, task.Retries, "Unable to deliver webhook: "+task.Result, "failed")
				continue
			}

			Instance, err := GetInstanceById(namePrefix, storage, task.ResourceId)
			if err != nil {
				UpdateTaskStatus(storage, task.Id, task.Retries+1, "Cannot get Instance: "+err.Error(), "pending")
				continue
			}
			if !IsAvailable(Instance.Status) {
				glog.Infof("Status did not change at provider for task: %s\n", task.Id)
				UpdateTaskStatus(storage, task.Id, task.Retries+1, "No change in status since last check", "pending")
				continue
			}

			byteData, err := json.Marshal(map[string]interface{}{"state": "succeeded", "description": "available"})
			// seems like this would be more useful, but whatevs: byteData, err := json.Marshal(Instance)

			if err != nil {
				UpdateTaskStatus(storage, task.Id, task.Retries, "Cannot marshal Instance to json: "+err.Error(), "pending")
				continue
			}

			var taskMetaData WebhookTaskMetadata
			err = json.Unmarshal([]byte(task.Metadata), &taskMetaData)
			if err != nil {
				glog.Infof("Cannot unmarshal task metadata to callback on create service: %s, %s\n", task.Id, err.Error())
				UpdateTaskStatus(storage, task.Id, task.Retries, "Cannot unmarshal task metadata to callback on create service: "+err.Error(), "pending")
				continue
			}

			h := hmac.New(sha256.New, []byte(taskMetaData.Secret))
			h.Write(byteData)
			sha := base64.StdEncoding.EncodeToString(h.Sum(nil))

			client := &http.Client{}
			req, err := http.NewRequest("POST", taskMetaData.Url, bytes.NewReader(byteData))
			if err != nil {
				UpdateTaskStatus(storage, task.Id, task.Retries+1, "Failed to create http post request: "+err.Error(), "pending")
				continue
			}
			req.Header.Add("content-type", "application/json")
			req.Header.Add("x-osb-signature", sha)
			resp, err := client.Do(req)
			if err != nil {
				UpdateTaskStatus(storage, task.Id, task.Retries+1, "Failed to send http post operation: "+err.Error(), "pending")
				continue
			}
			resp.Body.Close() // ignore it, we dont want to hear it.

			if os.Getenv("RETRY_WEBHOOKS") != "" {
				if resp.StatusCode < 200 || resp.StatusCode > 399 {
					UpdateTaskStatus(storage, task.Id, task.Retries+1, "Got invalid http status code from hook: "+resp.Status, "pending")
					continue
				}
				FinishedTask(storage, task.Id, task.Retries, resp.Status, "finished")
			} else {
				if resp.StatusCode < 200 || resp.StatusCode > 399 {
					UpdateTaskStatus(storage, task.Id, task.Retries+1, "Got invalid http status code from hook: "+resp.Status, "failed")
				} else {
					FinishedTask(storage, task.Id, task.Retries, resp.Status, "finished")
				}
			}
		} else if task.Action == ChangePlansTask {
			glog.Infof("Changing plans for database: %s\n", task.Id)
			if task.Retries >= 60 {
				glog.Infof("Retry limit was reached for task: %s %d\n", task.Id, task.Retries)
				FinishedTask(storage, task.Id, task.Retries, "Unable to change plans for database "+task.ResourceId+" as it failed multiple times ("+task.Result+")", "failed")
				continue
			}
			Instance, err := GetInstanceById(namePrefix, storage, task.ResourceId)
			if err != nil {
				glog.Infof("Failed to get provider instance for task: %s, %s\n", task.Id, err.Error())
				UpdateTaskStatus(storage, task.Id, task.Retries, "Cannot get Instance: "+err.Error(), "pending")
				continue
			}
			var taskMetaData ChangePlansTaskMetadata
			err = json.Unmarshal([]byte(task.Metadata), &taskMetaData)
			if err != nil {
				glog.Infof("Cannot unmarshal task metadata to change providers: %s, %s\n", task.Id, err.Error())
				UpdateTaskStatus(storage, task.Id, task.Retries+1, "Cannot unmarshal task metadata to change providers: "+err.Error(), "pending")
				continue
			}
			output, err := UpgradeWithinProviders(storage, Instance, taskMetaData.Plan, namePrefix)
			if err != nil {
				glog.Infof("Cannot change plans for: %s, %s\n", task.Id, err.Error())
				UpdateTaskStatus(storage, task.Id, task.Retries+1, "Cannot change plans: " + err.Error(), "pending")
				continue
			}

			FinishedTask(storage, task.Id, task.Retries, output, "finished")
		} else if task.Action == ChangeProvidersTask {
			glog.Infof("Changing providers for database: %s\n", task.Id)
			if task.Retries >= 60 {
				glog.Infof("Retry limit was reached for task: %s %d\n", task.Id, task.Retries)
				FinishedTask(storage, task.Id, task.Retries, "Unable to resync information from provider for database "+task.ResourceId+" as it failed multiple times ("+task.Result+")", "failed")
				continue
			}
			Instance, err := GetInstanceById(namePrefix, storage, task.ResourceId)
			if err != nil {
				glog.Infof("Failed to get provider instance for task: %s, %s\n", task.Id, err.Error())
				UpdateTaskStatus(storage, task.Id, task.Retries, "Cannot get Instance: " + err.Error(), "pending")
				continue
			}
			var taskMetaData ChangeProvidersTaskMetadata
			err = json.Unmarshal([]byte(task.Metadata), &taskMetaData)
			if err != nil {
				glog.Infof("Cannot unmarshal task metadata to change providers: %s, %s\n", task.Id, err.Error())
				UpdateTaskStatus(storage, task.Id, task.Retries, "Cannot unmarshal task metadata to change providers: "+err.Error(), "pending")
				continue
			}
			output, err := UpgradeAcrossProviders(storage, Instance, taskMetaData.Plan, namePrefix)
			if err != nil {
				glog.Infof("Cannot switch providers: %s, %s\n", task.Id, err.Error())
				UpdateTaskStatus(storage, task.Id, task.Retries, "Cannot switch providers: "+err.Error(), "pending")
				continue
			}

			FinishedTask(storage, task.Id, task.Retries, output, "finished")
		}
		// TODO: create binding NotifyCreateBindingWebhookTask

		glog.Infof("Finished task: %s\n", task.Id)
	}
	return nil
}

func RunBackgroundTasks(ctx context.Context, o Options) error {
	storage, namePrefix, err := InitFromOptions(ctx, o)
	if err != nil {
		return err
	}

	go TickTocPreprovisionTasks(ctx, o, namePrefix, storage)
	return RunWorkerTasks(ctx, o, namePrefix, storage)
}
