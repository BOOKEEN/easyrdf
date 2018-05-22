<?php

namespace EasyRdf\Sparql\Query;

use EasyRdf\Format;
use EasyRdf\Sparql\Client as sparqlClient;

class Client extends sparqlClient
{
    /** @var string */
    private $defaultGraphUri;

    /**
     * @param string $sparqlEndpoint
     * @param string|null $defaultGraphUri
     */
    public function __construct($sparqlEndpoint, $defaultGraphUri = null)
    {
        parent::__construct($sparqlEndpoint);

        if ($defaultGraphUri) {
            $this->defaultGraphUri = $defaultGraphUri;
        }
    }

    /**
     * Specify a default graph uri for a query Operation.
     * If default-graph-uri is provided. It will be used over FROM query statement (when specified)
     *
     * Empty default graph = remove
     *
     * @see https://www.w3.org/TR/2013/REC-sparql11-protocol-20130321/#dataset
     *
     * @param string $defaultGraphUri
     */
    public function setDefaultGraphUri($defaultGraphUri)
    {
        $this->defaultGraphUri = empty($defaultGraphUri) ? null : $defaultGraphUri;
    }

    /**
     * @inheritdoc
     */
    public function query($query)
    {
        if ($this->defaultGraphUri) {
            $this->addRdfDatasetParameter(sparqlClient::QUERY_PARAM_DEFAULT_GRAPH, $this->defaultGraphUri);
        }

        return parent::query($query);
    }
}