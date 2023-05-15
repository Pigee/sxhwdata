<?php

class nxswService extends EmployeeFactory
{

    public function __construct()
    {
        parent::__construct();
    }

    /**
     * 在下面可以自由扩展其他sql查询
     */

  

    /**
     * 个人总工单数
     */
   function _getzgdsCount($userid,$start_date,$end_date){
    $sql="SELECT count(*) FROM `wf_hist_task` where  `operator`='$userid' and `create_Time`  BETWEEN  '$start_date'  and '$end_date' ";
 
    $count = $this->get_var($sql);
    return $count;
    }

    /**
     *
     * @param null $report_mod 'gzbx'  'xxcx'  'tssl'  'jbjy'
     * @param null $department
     * @param null $userid
     * @param int $type 1=接单 2=处理
     * @return mixed|NULL
     */
    function _getCount($sess_id,$report_mod = null, $department = null, $userid = null, $type = 1)
    {
        $sql = "SELECT count(*) FROM `tmp_$sess_id` where  1=1";
        if (!empty($report_mod)) {
            $sql .= " and  report_mod='$report_mod'";
        }
        if (!empty($department)) {
            $sql .= " and  department='$department'";
        }
        if (!empty($userid)) {
            if ($type == 1) {
                $sql .= " and  person_receiver='$userid'";
            } else {
                $sql .= " and  person_handle='$userid'";
            }
        }
        $count = $this->get_var($sql);
        return $count;
    }

    /**
     *
     * @param null $report_mod 'gzbx'  'xxcx'  'tssl'  'jbjy'
     * @param null $department
     * @param null $userid
     * @param int $type 1=接单 2=处理
     * @return mixed|NULL
     */
    function _getBMYCount($sess_id, $report_mod = null, $department = null, $userid = null, $type = 1)
    {
        $sql = "SELECT count(*) FROM `tmp_$sess_id` where  appraise='不满意'";
        if (!empty($report_mod)) {
            $sql .= " and  report_mod='$report_mod'";
        }
        if (!empty($department)) {
            $sql .= " and  department='$department'";
        }

        if (!empty($userid)) {
            if ($type == 1) {
                $sql .= " and  person_receiver='$userid'";
            } else {
                $sql .= " and  person_handle='$userid'";
            }
        }
        $count = $this->get_var($sql);
        return $count;
    }


    /**
     *
     * @param null $report_mod 'gzbx'  'xxcx'  'tssl'  'jbjy'
     * @param null $department
     * @param null $userid
     * @param int $type 1=接单 2=处理
     * @return mixed|NULL
     */
    function _getHfCount($sess_id, $report_mod = null, $department = null, $userid = null, $type = 1)
    {
        $sql = "SELECT count(*) FROM `tmp_$sess_id` where time_appraise is not null";
        if (!empty($report_mod)) {
            $sql .= " and  report_mod='$report_mod'";
        }
        if (!empty($department)) {
            $sql .= " and  department='$department'";
        }
        if (!empty($userid)) {
            if ($type == 1) {
                $sql .= " and  person_receiver='$userid'";
            } else {
                $sql .= " and  person_handle='$userid'";
            }
        }
        $count = $this->get_var($sql);
        return $count;
    }

    /**
     * 处理及时率
     * @param null $report_mod 'gzbx'  'xxcx'  'tssl'  'jbjy'
     * @param null $department
     * @param null $userid
     * @param int $type 1=接单 2=处理
     * @return mixed|NULL
     */
    function _getNoChuliCount($sess_id, $report_mod = null, $department = null, $userid = null, $type = 1)
    {
        $sql = "SELECT count(*) FROM `tmp_$sess_id` where repair='小修'";
        if (!empty($report_mod)) {
            $sql .= " and  report_mod='$report_mod'";
        }
        if (!empty($department)) {
            $sql .= " and  department='$department'";
        }
        $sql .= " and ROUND((UNIX_TIMESTAMP(time_receiver)-UNIX_TIMESTAMP(time_accept))/3600)>8";
        if (!empty($userid)) {
            if ($type == 1) {
                $sql .= " and  person_receiver='$userid'";
            } else {
                $sql .= " and  person_handle='$userid'";
            }
        }
        $count = $this->get_var($sql);

        $sql2 = "SELECT count(*) FROM `tmp_$sess_id` where repair='大修'";
        if (!empty($report_mod)) {
            $sql2 .= " and  report_mod='$report_mod'";
        }
        if (!empty($department)) {
            $sql2 .= " and  department='$department'";
        }
        $sql2 .= " and ROUND((UNIX_TIMESTAMP(time_receiver)-UNIX_TIMESTAMP(time_accept))/3600)>24";
        if (!empty($userid)) {
            if ($type == 1) {
                $sql2 .= " and  person_receiver='$userid'";
            } else {
                $sql2 .= " and  person_handle='$userid'";
            }
        }
        $count2 = $this->get_var($sql2);
        return $count+$count2;
    }

    /**
     * 处理执行率
     * @param null $report_mod 'gzbx'  'xxcx'  'tssl'  'jbjy'
     * @param null $department
     * @param null $userid
     * @param int $type 1=接单 2=处理
     * @return mixed|NULL
     */
    function _getChaoshiCount($sess_id, $report_mod = null, $department = null, $userid = null, $type = 1)
    {
        $sql = "SELECT count(*) FROM `tmp_$sess_id` where 1=1";
        if (!empty($report_mod)) {
            $sql .= " and  report_mod='$report_mod'";
        }
        if (!empty($department)) {
            $sql .= " and  department='$department'";
        }

        $sql .= " and ROUND((UNIX_TIMESTAMP(time_receiver)-UNIX_TIMESTAMP(time_accept))/60)>20";

        if (!empty($userid)) {
            if ($type == 1) {
                $sql .= " and  person_receiver='$userid'";
            } else {
                $sql .= " and  person_handle='$userid'";
            }
        }
        $count = $this->get_var($sql);
        return $count;
    }



    function _getyclCount($sess_id, $report_mod = null, $department = null, $userid = null, $type = 1)
    {
       // if($report_mod == '信息查询业务咨询' ){
       //       $sql = "SELECT count(*) FROM `tmp_$sess_id` where (type ='是'  or gd_result is not null ) ";
       //  }else{
           $sql = "SELECT count(*) FROM `tmp_$sess_id` where (appraise is not null or gd_result is not null) ";
    //    }
        if (!empty($report_mod)) {
            $sql .= " and  report_mod='$report_mod'";
        }
        if (!empty($department)) {
            $sql .= " and  department='$department'";
        }

        if (!empty($userid)) {
            if ($type == 1) {
                $sql .= " and  person_receiver='$userid'";
            } else {
                $sql .= " and  person_handle='$userid'";
            }
        }
       // echo $sql;
        $count = $this->get_var($sql);
        return $count;
    }

    function getDepartmentList($sess_id)
    {
        $sql = "SELECT department FROM `tmp_$sess_id` where IFNULL(department,'') !='' GROUP BY department";
        return $this->get_results($sql, ARRAY_A);
    }

    function getList($sess_id)
    {
        $dep = $this->getDepartmentList($sess_id);
        $tmp = [];
        if (!empty($dep)) {
            foreach ($dep as $_dep) {
                $t = [];
                $dep_name = $_dep['department'];
                $t['strName'] = $dep_name;
                $t['count'] = $this->_getCount($sess_id,null,$dep_name);
                $t['nocl_count'] = $this->_getNoChuliCount($sess_id,null,$dep_name);
                $t['ycl_count'] = $this->_getyclCount($sess_id,null,$dep_name);
                $t['bmy_count'] = $this->_getBMYCount($sess_id,null,$dep_name);
                $t['hf_count'] = $this->_getHfCount($sess_id,null,$dep_name);
                $t['chaogshi_count'] = $this->_getChaoshiCount($sess_id,null,$dep_name);
                $t['my_count'] = $t['count'] - $t['bmy_count'];
                $tmp[] = $t;
            }
        }
        return $tmp;

    }

    function bmyList($sess_id)
    {
        $sql = "SELECT * FROM `tmp_$sess_id` where appraise='不满意' order by create_date asc";
        return $this->get_results($sql, ARRAY_A);
    }

    function chaoshiList($sess_id)
    {
        $sql = "SELECT * FROM `tmp_$sess_id` where 1=1 and  ROUND((UNIX_TIMESTAMP(time_receiver)-UNIX_TIMESTAMP(time_accept))/60)>20 order by create_date asc";
        return $this->get_results($sql, ARRAY_A);
    }

    function noList($sess_id)
    {
        $tmp=[];
        $sql = "SELECT * FROM `tmp_$sess_id` where repair='小修' and ROUND((UNIX_TIMESTAMP(time_receiver)-UNIX_TIMESTAMP(time_accept))/3600)>8";
        $rs=$this->get_results($sql, ARRAY_A);
        $rs2=$this->get_results("SELECT * FROM `tmp_$sess_id` where repair='大修' and ROUND((UNIX_TIMESTAMP(time_receiver)-UNIX_TIMESTAMP(time_accept))/3600)>24", ARRAY_A);
        if ($rs){
            foreach ($rs as $_rs){
                $tmp[]=$_rs;
            }
        }
        if ($rs2){
            foreach ($rs2 as $_rs2){
                $tmp[]=$_rs2;
            }
        }
        return $tmp;
    }

    function _getInfoList($sess_id)
    {
        $sql = "SELECT * FROM `tmp_$sess_id` where 1=1 order by create_date asc";
        return $this->get_results($sql, ARRAY_A);
    }

    function getDepartment()
    {
        $sql = "SELECT cs_base_department.departmentid,cs_base_department.`name` FROM cs_base_department WHERE cs_base_department.parentid = '189773cfd0bd4842b57fde93b120a8df'";
        return $this->get_results($sql, ARRAY_A);
    }


    /**
     * 创建临时表
     * @param $tabName
     */
    function _crrateTmpTable($sess_id,$start_date,$end_date)
    {
        $sql = "CREATE TEMPORARY TABLE `tmp_$sess_id` (
	`report_mod` VARCHAR ( 32 ) DEFAULT NULL,
	`work_no` VARCHAR ( 100 ) DEFAULT NULL,
	`create_date` datetime DEFAULT NULL,
	`hm` VARCHAR ( 100 ) DEFAULT NULL,
	`phone` VARCHAR ( 100 ) DEFAULT NULL,
	`dz_address` VARCHAR ( 200 ) DEFAULT NULL,
	`remark` VARCHAR ( 100 ) DEFAULT NULL,	
	`time_accept` VARCHAR ( 100 ) DEFAULT NULL,	
	`person_accept` VARCHAR ( 100 ) DEFAULT NULL,
	`department` VARCHAR ( 100 ) DEFAULT NULL,	
	`time_receiver` VARCHAR ( 100 ) DEFAULT NULL,
	`person_receiver` VARCHAR ( 100 ) DEFAULT NULL,
	`time_handle` VARCHAR ( 100 ) DEFAULT NULL,
	`person_handle` VARCHAR ( 200 ) DEFAULT NULL,	
	`gd_result` VARCHAR ( 200 ) DEFAULT NULL,
	`time_appraise` VARCHAR ( 200 ) DEFAULT NULL,
	`person_appraise` VARCHAR ( 200 ) DEFAULT NULL,
	`remark_appraise` VARCHAR ( 200 ) DEFAULT NULL,
    `appraise` VARCHAR ( 200 ) DEFAULT NULL,
    `repair` VARCHAR ( 200 ) DEFAULT NULL  
)";
        $this->query($sql);
        $sql="INSERT INTO `tmp_$sess_id` (SELECT
		'故障报修' AS `report_mod`,
		`w_1592374411638`.`work_no` AS `work_no`,
		`w_1592374411638`.`create_date` AS `create_date`,
		`w_1592374411638`.`hm` AS `hm`,
		`w_1592374411638`.`phone` AS `phone`,
		`w_1592374411638`.`dz_address` AS `dz_address`,
		`w_1592374411638`.`remark` AS `remark`,
		`w_1592374411638`.`time_accept` AS `time_accept`,
		`w_1592374411638`.`person_accept` AS `person_accept`,
		`w_1592374411638`.`department` AS `department`,
		`w_1592374411638`.`time_receiver` AS `time_receiver`,
		`w_1592374411638`.`person_receiver1` AS `person_receiver`,
		`w_1592374411638`.`time_handle` AS `time_handle`,
		`w_1592374411638`.`person_handle` AS `person_handle`,
		`w_1592374411638`.`gd_result` AS `gd_result`,
		`w_1592374411638`.`time_appraise` AS `time_appraise`,
		`w_1592374411638`.`person_appraise` AS `person_appraise`,
		`w_1592374411638`.`remark_appraise` AS `remark_appraise`,
		`w_1592374411638`.`appraise` AS `appraise`,
		`w_1592374411638`.`repair` AS `repair`
	FROM
		`w_1592374411638` where `w_1592374411638`.`create_date` BETWEEN DATE_FORMAT('$start_date','%Y-%m-%d %H:%i:%S') and DATE_FORMAT('$end_date','%Y-%m-%d %H:%i:%S') UNION ALL
	SELECT
		'信息查询业务咨询' AS `report_mod`,
		`nxsw_1609840221906`.`work_no` AS `work_no`,
		`nxsw_1609840221906`.`create_date` AS `create_date`,
		`nxsw_1609840221906`.`hm` AS `hm`,
		`nxsw_1609840221906`.`phone` AS `phone`,
		`nxsw_1609840221906`.`dz_address` AS `dz_address`,
		`nxsw_1609840221906`.`remark` AS `remark`,
		`nxsw_1609840221906`.`time_accept` AS `time_accept`,
		IFNULL( `nxsw_1609840221906`.w_1629992975899, IFNULL( `nxsw_1609840221906`.w_1629992993240, IFNULL( `nxsw_1609840221906`.w_1629993002387, IFNULL( `nxsw_1609840221906`.w_1629993009365, IFNULL( `nxsw_1609840221906`.w_1629993017404, `nxsw_1609840221906`.w_1629993032304 ) ) ) ) )  as person_accept,
		`nxsw_1609840221906`.`department` AS `department`,
		`nxsw_1609840221906`.`time_receiver` AS `time_receiver`,
		`nxsw_1609840221906`.`person_receiver1` AS `person_receiver`,
		`nxsw_1609840221906`.`time_handle` AS `time_handle`,
		`nxsw_1609840221906`.`person_handle` AS `person_handle`,
		`nxsw_1609840221906`.`gd_result` AS `gd_result`,
		`nxsw_1609840221906`.`time_appraise` AS `time_appraise`,
		`nxsw_1609840221906`.`person_appraise` AS `person_appraise`,
		`nxsw_1609840221906`.`remark_appraise` AS `remark_appraise`,
		`nxsw_1609840221906`.`appraise` AS `appraise`,
		'' AS `repair`
	FROM
		`nxsw_1609840221906` where `nxsw_1609840221906`.`create_date` BETWEEN DATE_FORMAT('$start_date','%Y-%m-%d %H:%i:%S') and DATE_FORMAT('$end_date','%Y-%m-%d %H:%i:%S') UNION ALL
	SELECT
		'投诉受理' AS `report_mod`,
		`nxsw_1609923147926`.`work_no` AS `work_no`,
		`nxsw_1609923147926`.`create_date` AS `create_date`,
		`nxsw_1609923147926`.`hm` AS `hm`,
		`nxsw_1609923147926`.`phone` AS `phone`,
		`nxsw_1609923147926`.`dz_address` AS `dz_address`,
		`nxsw_1609923147926`.`remark` AS `remark`,
		`nxsw_1609923147926`.`time_accept` AS `time_accept`,
		`nxsw_1609923147926`.`person_accept` AS `person_accept`,
		`nxsw_1609923147926`.`department` AS `department`,
		`nxsw_1609923147926`.`time_receiver` AS `time_receiver`,
		`nxsw_1609923147926`.`person_receiver` AS `person_receiver`,
		`nxsw_1609923147926`.`time_handle` AS `time_handle`,
		`nxsw_1609923147926`.`person_handle` AS `person_handle`,
		`nxsw_1609923147926`.`gd_result` AS `gd_result`,
		`nxsw_1609923147926`.`time_appraise` AS `time_appraise`,
		`nxsw_1609923147926`.`person_appraise` AS `person_appraise`,
		`nxsw_1609923147926`.`remark_appraise` AS `remark_appraise`,
		`nxsw_1609923147926`.`appraise` AS `appraise`,
		'' AS `repair`
	FROM
		`nxsw_1609923147926` where `nxsw_1609923147926`.`create_date` BETWEEN DATE_FORMAT('$start_date','%Y-%m-%d %H:%i:%S') and DATE_FORMAT('$end_date','%Y-%m-%d %H:%i:%S') UNION ALL
    -- SELECT
    --     '用水销户' AS `report_mod`,
    --     `nxsw_1609921051879`.`work_no` AS `work_no`,
    --     `nxsw_1609921051879`.`create_date` AS `create_date`,
    --     `nxsw_1609921051879`.`hm` AS `hm`,
    --     `nxsw_1609921051879`.`phone` AS `phone`,
    --     `nxsw_1609921051879`.`dz_address` AS `dz_address`,
    --     `nxsw_1609921051879`.`remark` AS `remark`,
    --     `nxsw_1609921051879`.`time_accept` AS `time_accept`,
    --     `nxsw_1609921051879`.`person_accept` AS `person_accept`,
    --     `nxsw_1609921051879`.`department` AS `department`,
    --     `nxsw_1609921051879`.`time_receiver` AS `time_receiver`,
    --     `nxsw_1609921051879`.`person_receiver` AS `person_receiver`,
    --     `nxsw_1609921051879`.`time_handle` AS `time_handle`,
    --     `nxsw_1609921051879`.`person_handle` AS `person_handle`,
    --     `nxsw_1609921051879`.`gd_result` AS `gd_result`,
    --     `nxsw_1609921051879`.`time_appraise` AS `time_appraise`,
    --     `nxsw_1609921051879`.`person_appraise` AS `person_appraise`,
    --     `nxsw_1609921051879`.`remark_appraise` AS `remark_appraise`,
    --     `nxsw_1609921051879`.`appraise` AS `appraise`,
    --     '' AS `repair`
    -- FROM
    --     `nxsw_1609921051879` where `nxsw_1609921051879`.`create_date` BETWEEN DATE_FORMAT('$start_date','%Y-%m-%d %H:%i:%S') and DATE_FORMAT('$end_date','%Y-%m-%d %H:%i:%S') UNION ALL
    -- SELECT
    --     '报装申请' AS `report_mod`,
    --     `nxsw_1604890018001`.`work_no` AS `work_no`,
    --     `nxsw_1604890018001`.`create_date` AS `create_date`,
    --     `nxsw_1604890018001`.`hm` AS `hm`,
    --     `nxsw_1604890018001`.`phone` AS `phone`,
    --     `nxsw_1604890018001`.`dz_address` AS `dz_address`,
    --     `nxsw_1604890018001`.`remark` AS `remark`,
    --     `nxsw_1604890018001`.`time_accept` AS `time_accept`,
    --     `nxsw_1604890018001`.`person_accept` AS `person_accept`,
    --     `nxsw_1604890018001`.`department` AS `department`,
    --     `nxsw_1604890018001`.`time_receiver` AS `time_receiver`,
    --     `nxsw_1604890018001`.`person_receiver` AS `person_receiver`,
    --     `nxsw_1604890018001`.`time_handle` AS `time_handle`,
    --     `nxsw_1604890018001`.`person_handle` AS `person_handle`,
    --     `nxsw_1604890018001`.`gd_result` AS `gd_result`,
    --     `nxsw_1604890018001`.`time_appraise` AS `time_appraise`,
    --     `nxsw_1604890018001`.`person_appraise` AS `person_appraise`,
    --     `nxsw_1604890018001`.`remark_appraise` AS `remark_appraise`,
    --     `nxsw_1604890018001`.`appraise` AS `appraise`,
    --     '' AS `repair`
    -- FROM
    --     `nxsw_1604890018001` where `nxsw_1604890018001`.`create_date` BETWEEN DATE_FORMAT('$start_date','%Y-%m-%d %H:%i:%S') and DATE_FORMAT('$end_date','%Y-%m-%d %H:%i:%S') UNION ALL
	SELECT
		'举报建议' AS `report_mod`,
		`nxsw_1609926366221`.`work_no` AS `work_no`,
		`nxsw_1609926366221`.`create_date` AS `create_date`,
		`nxsw_1609926366221`.`hm` AS `hm`,
		`nxsw_1609926366221`.`phone` AS `phone`,
		`nxsw_1609926366221`.`dz_address` AS `dz_address`,
		`nxsw_1609926366221`.`remark` AS `remark`,
		`nxsw_1609926366221`.`time_accept` AS `time_accept`,
		`nxsw_1609926366221`.`person_accept` AS `person_accept`,
		`nxsw_1609926366221`.`department` AS `department`,
		`nxsw_1609926366221`.`time_receiver` AS `time_receiver`,
		`nxsw_1609926366221`.`person_receiver` AS `person_receiver`,
		`nxsw_1609926366221`.`time_handle` AS `time_handle`,
		`nxsw_1609926366221`.`person_handle` AS `person_handle`,
		`nxsw_1609926366221`.`gd_result` AS `gd_result`,
		`nxsw_1609926366221`.`time_appraise` AS `time_appraise`,
		`nxsw_1609926366221`.`person_appraise` AS `person_appraise`,
		`nxsw_1609926366221`.`remark_appraise` AS `remark_appraise`,
		`nxsw_1609926366221`.`appraise` AS `appraise`,
		'' AS `repair` 
	FROM
	`nxsw_1609926366221` where `nxsw_1609926366221`.`create_date` BETWEEN DATE_FORMAT('$start_date','%Y-%m-%d %H:%i:%S') and DATE_FORMAT('$end_date','%Y-%m-%d %H:%i:%S') )";
        $this->query($sql);
    }


    /**
     * 删除临时表
     * @param $tabName
     */
    function _delTmpTable($sess_id)
    {
        $sql = "DROP TABLE `tmp_$sess_id`";
        $this->query($sql);
    }


  

}