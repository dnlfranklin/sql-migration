<?php

namespace SqlMigration\Provider\Mysql;

use SqlMigration\Catalog\Schema;
use SqlMigration\Core\Connector;

final class InformationSchema{

    public function __construct(private Connector $connector){}

    public function load(string $schema_name):?Schema{
        $pdo = $this->connector->get();

        $stmt = $pdo->prepare('SELECT * FROM information_schema.schemata WHERE schema_name = ?');
        $stmt->bindValue(1, $schema_name);
        $stmt->execute();
        $data = $stmt->fetch();
        
        if(!$data){
            return null;
        }

        return new Schema($data['SCHEMA_NAME'], $data['DEFAULT_CHARACTER_SET_NAME'], $data['DEFAULT_COLLATION_NAME']);
    }    

}