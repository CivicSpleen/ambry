DELETE FROM columns WHERE c_t_id IN 
   ( SELECT t_id FROM tables WHERE t_d_id = {d_id};   
DELETE FROM tables WHERE t_d_id = {d_id};
DELETE FROM partitions WHERE p_d_id =  {d_id};
DELETE FROM datasets WHERE d_id = {d_id}