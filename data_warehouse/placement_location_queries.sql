select focus_child_id, removal_episode_number, location_number_in_removal_episode, started_at, ended_at
from placement_locations
order by focus_child_id, started_at


select pl1.focus_child_id, pl1.removal_episode_number, 
       pl1.location_number_in_removal_episode, pl1.ended_at, 
       pl2.location_number_in_removal_episode, pl2.started_at, pl2.ended_at, 
       pl3.location_number_in_removal_episode, pl3.started_at
from placement_locations pl1, placement_locations pl2, placement_locations pl3
where pl1.focus_child_id = pl2.focus_child_id
and pl2.focus_child_id = pl3.focus_child_id
and pl1.removal_episode_number = pl2.removal_episode_number
and pl2.removal_episode_number = pl3.removal_episode_number
and pl1.location_number_in_removal_episode < (pl2.location_number_in_removal_episode - 1)
and pl3.location_number_in_removal_episode > (pl2.location_number_in_removal_episode + 1)


select non_placement_child_locations.focus_child_id, non_placement_child_locations.removal_episode_number,
       non_placement_child_locations.move_number_ever, non_placement_child_locations.move_number_in_removal_episode
from child_locations non_placement_child_locations
where not exists (select 1 from placement_locations pl
                  where pl.focus_child_id = non_placement_child_locations.focus_child_id
                  and pl.removal_episode_number = non_placement_child_locations.removal_episode_number
                  and pl.started_at < non_placement_child_locations.started_at
                  and pl.ended_at > non_placement_child_locations.ended_at)

select non_placement_child_locations.focus_child_id, before_placement.ended_at, non_placement_child_locations.started_at,
non_placement_child_locations.ended_at, after_placement.started_at
from child_locations non_placement_child_locations, placement_locations before_placement, placement_locations after_placement
where non_placement_child_locations.focus_child_id = before_placement.focus_child_id
and non_placement_child_locations.focus_child_id = after_placement.focus_child_id
and before_placement.ended_at = non_placement_child_locations.started_at
and non_placement_child_locations.ended_at = after_placement.started_at
and non_placement_child_locations.ended_at > non_placement_child_locations.started_at
and non_placement_child_locations.removal_episode_number = before_placement.removal_episode_number
and non_placement_child_locations.removal_episode_number = after_placement.removal_episode_number
and not exists (select 1 from placement_locations pl
                  where pl.focus_child_id = non_placement_child_locations.focus_child_id
                  and pl.removal_episode_number = non_placement_child_locations.removal_episode_number
                  and pl.started_at < non_placement_child_locations.started_at
                  and pl.ended_at > non_placement_child_locations.ended_at)

select before_placement.focus_child_id, before_placement.ended_at, after_placement.started_at
from placement_locations before_placement, placement_locations after_placement
where before_placement.focus_child_id = after_placement.focus_child_id
and before_placement.removal_episode_number = after_placement.removal_episode_number
and after_placement.location_number_in_removal_episode = (before_placement.location_number_in_removal_episode + 1)
and after_placement.started_at > before_placement.ended_at


select count(*) from child_locations
select count(*) from placement_locations
