<?php

namespace SqlMigration;

use SqlMigration\Core\Connector;
use SqlMigration\Core\Differ;

class Factory{

    const AVAILABLE_PROVIDERS = [
        'mysql' => 'SqlMigration\Provider\Mysql\Migrator'
    ];

    private $provider;
    private Connector $connector;   

    public function __construct(Array $connection_info, string $provider = 'mysql'){
        if(!array_key_exists($provider, self::AVAILABLE_PROVIDERS)){
            throw new \Exception("Database provider not found");           
        }
        
        $this->provider  = self::AVAILABLE_PROVIDERS[$provider];
        $this->connector = new Connector($connection_info);
    }

    public function getDiff(Migration $migration):Array {
        $provider = $this->provider;
        
        $migrator = new $provider($this->connector, $migration->up());

        $differ = new Differ($migration->up(), $migrator->map());
        
        $diffs = [];

        foreach($differ->analyze() as $diff){
            if($diff['action']){
                $diffs[] = $diff['action'].' '.$diff['title'];
            }
        }

        return $diffs;
    }

    public function getSQL(Migration $migration, bool $migrate_data = false):Array {
        $provider = $this->provider;
        
        $migrator = new $provider($this->connector, $migration->up());    
        
        $sql = [];

        foreach($migrator->sqlInstructions($migrate_data) as $group){
            foreach($group as $cmd){
                $sql[]= $cmd;
            }
        }
        
        return $sql;
    }

    public function execute(Migration $migration, bool $migrate_data = false):Array {
        $provider = $this->provider;
        
        $migrator = new $provider($this->connector, $migration->up());    
        
        return $migrator->migrate($migrate_data);
    }

}