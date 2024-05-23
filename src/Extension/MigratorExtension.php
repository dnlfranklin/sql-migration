<?php

namespace SqlMigration\Extension;

use SqlMigration\Catalog\Schema;

interface MigratorExtension{
    
    public function map():Schema|null;

    public function sqlInstructions(bool $migrate_data = false):Array;
    
    public function migrate(bool $migrate_data = false):Array;

}