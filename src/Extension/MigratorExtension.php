<?php

namespace SqlMigration\Extension;

use SqlMigration\Catalog\Schema;
use SqlMigration\Core\Instruction;

interface MigratorExtension{
    
    public function map():Schema|null;

    public function getInstruction():Instruction;

}