<?php

namespace EasyRdf\Sparql\Response;

use EasyRdf\Http\Response;

class JsonLd
{

    /**
     * @param Response $response
     *
     * @return \stdClass
     */
    public function __construct(Response $response)
    {
        return $this->normalizeJsonLd($response);
    }

    /**
     * Normalize Json LD
     * (currently works with virtuoso 6 Json LD format)
     *
     * @param Response $response
     *
     * @return \stdClass|null
     */
    protected function normalizeJsonLd(Response $response)
    {
        $responseBody = $response->getBody();
        $bodyContent = json_decode($responseBody, true);

        if (empty($bodyContent)) {

            return;
        }

        if (isset($bodyContent['@graph'])) {

            return $bodyContent;
        } elseif (isset($bodyContent['@'])) {
            $jsonLd = new \stdClass();
            $jsonLd->{'@graph'} = array();
            foreach ($bodyContent['@'] as $graph) {
                $newNode = new \stdClass();
                foreach ($graph as $subject => $node) {
                    if ($subject === '@') {
                        $newNode->{'@id'} = $node;
                    } elseif ($subject === 'a') {
                        $newNode->{'@type'} = $node;
                    } else {
                        $this->getNodeValue($node, $subject, $newNode);
                    }
                }
                $jsonLd->{'@graph'}[] = $newNode;
            }

            return $jsonLd;
        } else {

            return;
        }
    }

    /**
     * @param array $node
     * @param string $subject
     * @param \stdClass $newNode
     */
    private function getNodeValue($node, $subject, &$newNode)
    {
        foreach ($node as $value) {
            if (\is_int($value) || \is_bool($value)) {
                $newNode->$subject = $value;
            } elseif (\is_string($value)) {
                $newSubject = new \stdClass();
                $newSubject->{'@id'} = $value;
                $newNode->$subject = $newSubject;
            } elseif (\is_array($value)) {
                $newSubject = new \stdClass();
                if (isset($value['@literal'])) {
                    $newSubject->{'@value'} = $value['@literal'];
                }
                if (isset($value['@language'])) {
                    $newSubject->{'@language'} = $value['@language'];
                }
                $newNode->$subject = $newSubject;
            }
        }
    }
}