<?php

namespace SqlMigration\Provider\Mysql;

use SqlMigration\Catalog\Data;
use SqlMigration\Core\Connector;

final class InformationData{

    public function __construct(private Connector $connector){}

    public function load(string $schema_name, string $table_name):?Data{
        $pdo = $this->connector->get();

        $stmt = $pdo->prepare("SELECT * FROM information_schema.tables WHERE table_schema = ? AND table_name = ? LIMIT 1;");
        $stmt->bindValue(1, $schema_name);
        $stmt->bindValue(2, $table_name);
        $stmt->execute();
        
        if(empty($stmt->fetch())){
            return null;
        }

        $stmt = $pdo->prepare("SELECT * FROM {$schema_name}.{$table_name}");
        $stmt->execute();
        $result = $stmt->fetchAll();

        if(empty($result)){
            return null;
        }

        $data = new Data($table_name);
        call_user_func_array([$data, 'setColumns'], array_keys($result[0]));
        
        foreach($result as $row){
            call_user_func_array([$data, 'add'], array_values($row));
        }

        return $data;
    }    

}