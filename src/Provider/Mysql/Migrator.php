<?php 

namespace SqlMigration\Provider\Mysql;

use SqlMigration\Catalog\Schema;
use SqlMigration\Core\Connector;
use SqlMigration\Core\Instruction;
use SqlMigration\Extension\MigratorExtension;

final class Migrator implements MigratorExtension{

    private Connector $connector;
    private Schema $schema_up;

    public function __construct(Connector $connector, Schema $schema_up){
        $this->connector = $connector;
        $this->schema_up = $schema_up;
    }

    public function map():Schema|null {
        $information_schema = new InformationSchema($this->connector);
        
        $schema = $information_schema->load($this->schema_up->name);

        if(!$schema){
            return null;
        }

        $information_table = new InformationTable($this->connector);

        foreach($information_table->load($this->schema_up->name) as $table){
            $schema->addTable($table);
        }

        $information_view = new InformationView($this->connector);

        foreach($information_view->load($this->schema_up->name) as $view){
            $schema->addView($view);
        }

        $information_data = new InformationData($this->connector);

        foreach($this->schema_up->data as $schema_data){
            $data = $information_data->load($this->schema_up->name, $schema_data->table_name);

            if($data){
                $schema->newData($data);
            }
        }

        return $schema;
    }

    public function getInstruction():Instruction {
        return new Instruction($this->schema_up);
    }   

}