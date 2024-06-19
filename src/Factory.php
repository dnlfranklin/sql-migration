<?php

namespace SqlMigration;

use SqlMigration\Core\Comparator;
use SqlMigration\Core\Connector;
use SqlMigration\Core\Executor;
use SqlMigration\Core\Thresher;

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

        $differ = Comparator::compare($migration->up(), $migrator->map());
        
        return $differ->get();
    }

    public function getSQL(Migration $migration, bool $trash = false):Array {
        $provider = $this->provider;
        
        $migrator = new $provider($this->connector, $migration->up());    
        
        if($trash){
            $differ = Thresher::clean($migration->up(), $migrator->map());
        }
        else{
            $differ = Comparator::compare($migration->up(), $migrator->map());
        }
        
        $instruction = $differ->inject($migrator->getInstruction());

        return $instruction->getSql();
    }

    public function execute(Migration $migration):Array {
        $provider = $this->provider;
        
        $schema_up = $migration->up();

        $migrator = new $provider($this->connector, $schema_up);    
        
        $differ = Comparator::compare($schema_up, $migrator->map());
        
        $instruction = $differ->inject($migrator->getInstruction());

        $executor = new Executor(
            $this->connector,
            $schema_up->foreign_key_check
        );
        
        $executables = $instruction->getExecutables();

        foreach($executables as $executable){
            $executor->execute($executable);

            if(!$executable->hasSuccess()){
                break;
            }
        }

        return $executables;
    }

}