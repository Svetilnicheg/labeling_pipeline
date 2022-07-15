import datetime
import tqdm
import pandas as pd
import toloka.client as toloka
import os

def create_new_toloka_pool(pool_id, fileDate, toloka_client):

    date_today = datetime.datetime.today().strftime('%Y%m%d')

    new_pool = toloka_client.clone_pool(
        pool_id=pool_id
    )
    new_pool.private_name = date_today+'_'+fileDate
    toloka_client.update_pool(pool_id=new_pool.id, pool=new_pool)
    return new_pool

# pool_id = 33388696



def add_tasks(pool_id, main_task_df, toloka_client):

    for i in tqdm.tqdm(range(0, len(main_task_df))):
        try:
            mt_dict = main_task_df.loc[i].to_dict()
            task = toloka.task.Task(
                input_values=mt_dict,
                pool_id=pool_id
            )
            # print(task)
            toloka_client.create_task(task=task, allow_defaults=True)
        except:
            print(main_task_df.loc[i])



def add_controls(toloka_client, control_task, pool_id):
    for i in tqdm.tqdm(range(0, len(control_task))):
        task = toloka.task.Task(
                input_values=control_task.loc[i, control_task.columns != 'result'].to_dict(),
                known_solutions=[
                    toloka.task.BaseTask.KnownSolution(
                        output_values=control_task.loc[i, control_task.columns == 'result'].to_dict())],
                pool_id=pool_id,
            infinite_overlap=True
            )
        toloka_client.create_task(task=task, allow_defaults=True)

def add_tasks_batch(pool_id, main_task_df, toloka_client):
    tasks = []
    for i in tqdm.tqdm(range(0, len(main_task_df))):
        mt_dict = main_task_df.loc[i].to_dict()
        task = toloka.task.Task(
            input_values=mt_dict,
            pool_id=pool_id
            )
        tasks.append(task)
    toloka_client.create_tasks(tasks=tasks, allow_defaults=True, skip_invalid_items=True)



def get_agg_results_from_toloka(toloka_client, pool_id, skill_id, agg_field_name):
    aggregation_operation = toloka_client.aggregate_solutions_by_pool(
        type=toloka.aggregation.AggregatedSolutionType.DAWID_SKENE,
        pool_id=pool_id,  # Aggregate in this pool
        answer_weight_skill_id=skill_id,  # Aggregate by this skill
        fields=[toloka.aggregation.PoolAggregatedSolutionRequest.Field(name=agg_field_name)]  # Aggregate this field
    )
    aggregation = toloka_client.wait_operation(aggregation_operation)
    print(aggregation)
    aggregation_results = list(toloka_client.get_aggregated_solutions(aggregation.id))
    results = pd.DataFrame()
    for res in tqdm.tqdm(aggregation_results):
        result = pd.DataFrame()
        task = toloka_client.get_task(task_id=res.task_id)
        result.loc[0, 'confidence'] = res.confidence
        for key in task.input_values.keys():
            result.loc[0, key] = task.input_values[key]
        for key in res.output_values.keys():
            result.loc[0, key] = res.output_values[key]
        results = results.append(result)
    return results


def get_pool_status(toloka_client, pool_id):
    if (str(toloka_client.get_pool(pool_id=pool_id).status) == 'Status.CLOSED' ) & (str(toloka_client.get_pool(pool_id=pool_id).last_close_reason) == 'CloseReason.COMPLETED'):
        print('Пул {} размечен'.format(pool_id))
        return pool_id
    else:
        print('Пул {} в работе'.format(pool_id))


def send_to_toloka(toloka_client, project, filedate, to_toloka_df, config):
    to_toloka_df.reset_index(drop=True, inplace=True)
    control_task = pd.read_csv(config['toloka'][project]['control_task_path'], sep='\t')

    new_pool = create_new_toloka_pool(pool_id=config['toloka'][project]['base_pool'],
                                                       fileDate=str(filedate),
                                                       toloka_client=toloka_client)
    add_tasks_batch(pool_id=new_pool.id,
                                     main_task_df=to_toloka_df,
                                     toloka_client=toloka_client)
    add_controls(toloka_client=toloka_client,
                                  control_task=control_task,
                                  pool_id=new_pool.id)
    toloka_client.open_pool(pool_id=new_pool.id)
    with open(config['toloka']['poolsDates_path'], 'a') as file:
        file.write('{};{};{};{}\n'.format(new_pool.id, filedate, project, 'labeling'))
    return new_pool.id



def get_from_toloka(toloka_client, project, filedate, config, pool_id):
    if not os.path.exists(config['toloka']['toloka_results_path'] + '/' + str(filedate) + '/' + project):
        os.makedirs(config['toloka']['toloka_results_path'] + '/' + str(filedate) + '/' + project)
    result = get_agg_results_from_toloka(toloka_client,
                                         int(pool_id),
                                         config['toloka'][project]['SKILL_ID'],
                                         config['toloka'][project]['AGG_FIELD_NAME'])
    result.to_csv(config['toloka']['toloka_results_path'] + '/' + str(filedate) + '/' + '{}/toloka_results_{}.csv'.format(project, filedate),
                  sep=';', index=False)
    return result