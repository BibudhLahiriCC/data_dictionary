select distinct(youth_stratum)
from ages 
order by youth_stratum

select *, occurred_on_id
from caseworker_focus_child_contacts 
limit 1

select distinct(cb2_contact_type)
from caseworker_focus_child_contacts 
limit 1

select *
from caseworkers

select *
from caseworker_focus_child_contacts 
where cb2_contact_id = 7069695

select *
from dates
where id = 2455511

select *
from caseworkers
limit 1

select child_age_at_start_in_days
from child_locations
where child_age_at_start_in_days is not null


select date, count(*)
from dates
group by date
having count(*) > 1
order by count(*) desc


select start_dates.date, end_dates.date, duration_in_days, end_dates.sql_date - start_dates.sql_date
from child_locations, dates start_dates, dates end_dates
where child_locations.start_date_id = start_dates.id
and child_locations.end_date_id = end_dates.id
limit 1


select familial_relationship_id
from child_locations
limit 10

select *
from familial_relationships

select count(*)
from child_locations
where familial_relationship_id is not null


select description, count(*)
from child_locations, familial_relationships
where familial_relationship_id = familial_relationships.id
group by description
order by count(*) desc

select provider_id
from child_locations
where provider_id is not null
limit 10


select removal_episode_id, count(*)
from child_locations
where removal_episode_end_date_id is not null
group by removal_episode_id
having count(*) > 1
order by count(*) desc


select *
from child_locations
where removal_episode_id = 4136370


select count(*)
from dates

select *
from dates
limit 40

select to_date('1900-01-01', 'YYYY-MM-DD') - 2415021

select distinct(weekday_indicator) 
from dates

select distinct(cb2_outcome_type)
from episode_ending_hearings
order by cb2_outcome_type

select *
from episode_ending_hearings
where id = 7265

--50859
select count(*)
from removal_episodes
where episode_ending_hearing_id is not null

--127
select count(distinct episode_ending_hearing_id)
from removal_episodes
where episode_ending_hearing_id is not null

select episode_ending_hearing_id, count(*)
from removal_episodes
group by episode_ending_hearing_id
order by count(*) desc

select *
from familial_relationships

select distinct(permanency_outcome)
from episode_ending_hearings
where state = 'Legacy'

select distinct(cb2_outcome_type)
from episode_ending_hearings
order by cb2_outcome_type

select distinct(single_or_couple)
from family_structures

select *
from family_structures

select distinct(current_involvement_name)
from focus_children
order by current_involvement_name

select distinct(description)
from location_types
order by description

select distinct(cb2_location_type)
from location_types
where cb2_location_sub_type is not null
order by cb2_location_type

select cb2_location_type, cb2_location_sub_type, count(*)
from location_types
group by cb2_location_type, cb2_location_sub_type
order by count(*) desc

select distinct(location_type)
from location_types
order by location_type

select distinct(cb2_provider_type)
from providers
order by cb2_provider_type

select distinct(provider_type)
from providers
where provider_sub_type in ('Expected', 'Data Error')
order by provider_type

select distinct(provider_sub_type)
from providers
order by provider_sub_type

select provider_type, cb2_provider_type, count(*)
from providers
group by provider_type, cb2_provider_type

select distinct(description)
from providers
order by description

select distinct(provider_sub_type)
from providers
order by provider_sub_type

select *
from removal_episodes, dates start_dates, dates end_dates
where duration_in_days = 1
and removal_episodes.start_date_id = start_dates.id
and removal_episodes.end_date_id = end_dates.id

select distinct(permanency_outcome)
from episode_ending_hearings
order by permanency_outcome

select pg_size_pretty(pg_database_size('analytics'))

select *
from providers

select *
from reason_for_removals


select reason_for_removal_id, count(*)
from removal_episodes
group by reason_for_removal_id
having count(*) > 1


select *, lower(period_ids), upper(period_ids)
from home_settings
limit 1

select distinct(name)
from home_settings
order by name

select *
from dates
where sql_date = current_date
limit 1

select *, lower(period_ids), upper(period_ids)
from home_settings
where upper(period_ids) < 2456344

select count(*) 
from home_settings

select distinct(name)
from involvements
order by name

select *
from involvements
where upper(period_ids) is null

select *
from last_contact_30day_trends
order by sql_date

select distinct(home_setting_name)
from last_contact_30day_trends
order by home_setting_name

select *
from last_contacts
limit 1

select *
from home_settings
where focus_child_id = 130991

select distinct(home_setting_name)
from last_contacts
order by home_setting_name

select *
from dates 
where id = 2456190

select focus_child_id, count(*)
from last_contacts
group by focus_child_id
order by count(*) desc

select *
from last_contacts
where focus_child_id = 247281
order by period_ids

select familial_relationship_id, count(*)
from child_locations
group by familial_relationship_id
order by familial_relationship_id

select *
from familial_relationships

select permanency_outcome, count(*)
from episode_ending_hearings
group by permanency_outcome
order by permanency_outcome

select current_home_setting, count(*)
from focus_children
group by current_home_setting
order by current_home_setting

select current_involvement_name, count(*)
from focus_children
group by current_involvement_name
order by current_involvement_name

select *
from dates 
limit 1

select period_ids
from involvements
limit 5

select distinct(cb2_contact_type) 
from caseworker_focus_child_contacts

select *
from cases 
limit 1

select distinct(state) 
from cases
order by state

select days_since_start_of_episode, removal_episode_start_date_id, 
rem_ep_start_date.sql_date, current_date - rem_ep_start_date.sql_date,
loc_start_date.sql_date, loc_start_date.sql_date - rem_ep_start_date.sql_date,
start_date_id - removal_episode_start_date_id
from placement_locations, dates rem_ep_start_date, dates loc_start_date
where placement_locations.removal_episode_start_date_id = rem_ep_start_date.id
and placement_locations.start_date_id = loc_start_date.id
limit 10


select duration_in_days, end_date_id, ended_at, end_date_id - start_date_id
from placement_locations
where ended_at is not null
limit 10


select number_of_contacts
from placement_locations
where number_of_contacts> 0
limit 10

select duration_in_days
from removal_locations
where duration_in_days is not null


